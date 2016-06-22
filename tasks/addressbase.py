import boto3
import boto3.s3.transfer
import csv
import logging
import luigi
import luigi.s3
import luigi.contrib.ftp
import luigi.contrib.spark
import operator
import os
from os import listdir
from os.path import isfile, join
import yaml

# Use a copy of the luigi.contrib.ftp module with a couple of hacks and additions
# to workaround some issues with OS FTP site.
import tasks.luigi_ftp

BUCKET = 'addressbase'
USRN_INDEX = 3
UPRN_INDEX = 3


class addressbase(luigi.Config):
    cache_dir = luigi.Parameter()
    from_os_dir = luigi.Parameter()
    from_os_csv_dir = luigi.Parameter()
    count_dir = luigi.Parameter()
    merged_dir = luigi.Parameter()
    usrn_grouped_dir = luigi.Parameter()
    uprn_grouped_dir = luigi.Parameter()
    schema_file = luigi.Parameter()
    location_records_filter = luigi.Parameter()
    include_headers = luigi.BoolParameter(default=False)


class AddressbaseFile(luigi.ExternalTask):
    """ A source Addressbase file, either a CSV or ZIP file.

    It is assumed to already exist.
    """

    file_path = luigi.Parameter()

    def output(self):
        if self.file_path.endswith('.csv'):
            return luigi.LocalTarget(self.file_path)
        else:
            return luigi.LocalTarget(self.file_path, format=luigi.format.Gzip)


# TODO: this doesnt work as OS FTP server returns full paths, not just
# filenames. So name does not match and luigi cannot find the remote file.
class FTPFile(luigi.ExternalTask):
    path = luigi.Parameter()
    host = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    # cache_dir = luigi.Parameter(default=addressbase().cache_dir)

    def output(self):
        # return luigi.contrib.ftp.RemoteTarget(self.path, self.host,
        return tasks.luigi_ftp.RemoteTarget(self.path, self.host,
                        username=self.username, password=self.password)


class AddressbaseS3File(luigi.ExternalTask):
    """ A source Addressbase file on S3.

    It is assumed to already exist.
    """

    file_path = luigi.Parameter()

    def output(self):
        return luigi.s3.S3Target(self.file_path)


#------------------------------------------------------------------------------

class GetFTPFile(luigi.Task):
    path = luigi.Parameter()
    host = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    local_path = luigi.Parameter()

    def requires(self):
        return FTPFile(self.path, self.host, self.username, self.password)

    def output(self):
        name = os.path.basename(self.path)
        self.output_path = join(self.local_path, name)
        return luigi.LocalTarget(self.output_path)

    def run(self):
        ftp_in = self.input()
        ftp_in.get(self.output_path)


class GetAllFTPFiles(luigi.WrapperTask):
    path = luigi.Parameter()
    host = luigi.Parameter()
    username = luigi.Parameter()
    password = luigi.Parameter()
    local_path = luigi.Parameter()

    def requires(self):
        fs = tasks.luigi_ftp.RemoteFileSystem(self.host, self.username, self.password)
        files = fs.listdir(self.path)
        return [GetFTPFile(f, self.host, self.username, self.password, self.local_path) for f in files]


#------------------------------------------------------------------------------

class UnzipFiles(luigi.contrib.external_program.ExternalProgramTask):
    def program_args(self):
        return ['unzip', 'cache/from-os/*', '-d', 'cache/from-os-csv']


#------------------------------------------------------------------------------

class GetS3File(luigi.Task):
    cache_dir = luigi.Parameter(default=addressbase().cache_dir)
    input_file = luigi.Parameter()
    output_file = luigi.Parameter()

    # def requires(self):
    #     return AddressbaseS3File(self.input_file)

    def output(self):
        self.dest_dir = join(self.cache_dir, 'records')
        self.output_path = join(self.dest_dir, self.output_file)
        return luigi.LocalTarget(self.output_path)

    def run(self):
        if not os.path.exists(self.dest_dir):
            os.makedirs(self.dest_dir)
        client = boto3.client('s3')
        transfer = boto3.s3.transfer.S3Transfer(client)
        output_file = join(self.cache_dir, self.output_file)
        transfer.download_file(BUCKET, self.input_file, self.output_path)


#------------------------------------------------------------------------------

class CountRecords(luigi.Task):
    file_in = luigi.Parameter()
    count_dir = luigi.Parameter(default=addressbase().count_dir)

    def requires(self):
        return AddressbaseFile(self.file_in)

    def output(self):
        filename = os.path.basename(self.file_in) + '.yml'
        return luigi.LocalTarget(join(self.count_dir, filename))

    def run(self):
        ids = {}
        with self.input().open('r') as f:
            for line in f:
                r_id = line[0:2]
                if ids.has_key(r_id):
                    ids[r_id] += 1
                else:
                    ids[r_id] = 1

        with self.output().open('w') as out:
            out.write(yaml.dump(ids, default_flow_style=False))


class CountAllRecords(luigi.Task):
    """ Creates a manifest of the Addressbase files.

    Lists all the files and how many record types are in each one.
    """

    from_os_csv_dir = luigi.Parameter(default=addressbase().from_os_csv_dir)
    count_dir = luigi.Parameter(default=addressbase().count_dir)

    def requires(self):
        files = [f for f in listdir(self.from_os_csv_dir) if (f.endswith('.csv') or f.endswith('.zip'))]
        return [CountRecords(join(self.from_os_csv_dir, f)) for f in files]

    def output(self):
        return luigi.LocalTarget(join(self.from_os_csv_dir, 'manifest.yml'))

    def run(self):
        countfiles = [f for f in listdir(self.count_dir) if f.endswith('.yml')]
        manifest = []

        for cf in countfiles:
            counts = yaml.load(open(join(self.count_dir, cf)))
            config = {'name': cf[:-4], 'counts': counts}
            manifest.append(config)

        with self.output().open('w') as out:
            out.write(yaml.dump(manifest, default_flow_style=False))


#------------------------------------------------------------------------------

class SplitRecords(luigi.Task):
    from_os_dir = luigi.Parameter()
    dest = luigi.Parameter()
    config = luigi.DictParameter()
    include_headers = luigi.BoolParameter(default=False)

    def requires(self):
        return CountAllRecords(self.from_os_dir)

    def output(self):
        return luigi.LocalTarget(join(self.dest, self.config['dest_name']))

    def run(self):
        file_in = join(self.from_os_dir, self.config['name'])
        lines = open(file_in).readlines()
        # Remove first and last two lines, as they are unused records
        lines = lines[1:-2]

        with self.output().open('w') as out:
            if self.include_headers:
                headers = ','.join(self.config['schema']['schema'])
                out.write(headers + "\n")

            # TODO: improve speed
            for l in lines:
                if l.startswith(str(self.config['schema']['id'])):
                    out.write(l)


class SplitAllRecords(luigi.Task):
    from_os_dir = luigi.Parameter(default=addressbase().from_os_dir)
    schema_file = luigi.Parameter()
    location_records_filter = luigi.Parameter()

    def requires(self):
        return CountAllRecords(self.from_os_dir)

    def run(self):
        manifest = yaml.load(self.input().open('r'))
        schema = yaml.load(open(self.schema_file, 'r'))
        records_filter = self.location_records_filter.split(',')
        for config in manifest:
            # Remove unwanted counts that are not important
            for num in ['10', '29', '99']:
                del config['counts'][num]

            # Most files have 1 key, a few have 2
            for k in config['counts'].iterkeys():
                # Only process the record types that are needed
                if k in records_filter:
                    config['schema'] = schema[k]
                    config['dest_name'] = config['name'].replace('.csv', '_r'+k+'.csv')
                    yield SplitRecords(from_os_dir=self.from_os_dir, config=config)


#------------------------------------------------------------------------------

class MergeRecords(luigi.Task):
    """ Merges records of a single type. """

    schema_file = luigi.Parameter(default=addressbase().schema_file)
    from_os_csv_dir = luigi.Parameter(default=addressbase().from_os_csv_dir)
    merged_dir = luigi.Parameter(default=addressbase().merged_dir)
    record_id = luigi.Parameter()
    include_headers = luigi.BoolParameter(default=False)
    addressbase_inputs = []

    def requires(self):
        return CountAllRecords(self.from_os_csv_dir)

    def output(self):
        manifest = yaml.load(self.input().open('r'))
        schema_data = yaml.load(open(self.schema_file, 'r'))
        self.record_def = schema_data[self.record_id]
        dest_name = 'Addressbase_' + self.record_def['name'] + '.csv'
        return luigi.LocalTarget(join(self.merged_dir, dest_name))

    def _create_inputs(self):
        manifest = yaml.load(self.input().open('r'))
        inputs = []
        for config in manifest:
            # Remove unwanted counts that are not important
            for num in ['10', '29', '99']:
                if config['counts'].has_key(num):
                    del config['counts'][num]
            if str(self.record_id) in config['counts'].keys():
                file_target = join(self.from_os_csv_dir, config['name'])
                # print file_target
                inputs.append({'path': file_target, 'mixed': config['counts'].keys() > 1})
        return inputs

    def _create_combined_output(self):
        with self.output().open('w') as out:
            if self.include_headers:
                headers = ','.join(self.record_def['schema'])
                out.write(headers + "\n")

            for file_in in self.addressbase_inputs:
                logging.info('Reading input file: ' + file_in['path'])
                lines = open(file_in['path'], 'r').readlines()
                # Remove first and last two lines, as they are unused records
                lines = lines[1:-2]
                if file_in['mixed']:
                    for l in lines:
                        if l.startswith(str(self.record_id)):
                            out.write(l)
                else:
                    out.writelines(lines)
                out.flush()

    def run(self):
        self.addressbase_inputs = self._create_inputs()
        # for t in self.inpuf_defs:
            # yield must occur inside run()!
            # target = yield AddressbaseFile(file_in=t)
            # self.addressbase_inputs.append(target)
        self._create_combined_output()


class MergeAllRecords(luigi.Task):
    from_os_csv_dir = luigi.Parameter(default=addressbase().from_os_csv_dir)
    schema_file = luigi.Parameter(default=addressbase().schema_file)
    location_records_filter = luigi.Parameter(default=addressbase().location_records_filter)

    def requires(self):
        return CountAllRecords(self.from_os_csv_dir)

    def run(self):
        manifest = yaml.load(self.input().open('r'))
        schema = yaml.load(open(self.schema_file, 'r'))
        records_filter = self.location_records_filter.split(',')

        for record_id in records_filter:
            yield MergeRecords(record_id=record_id)


#------------------------------------------------------------------------------
# Takes large Addressbase files containing single record types and sorts
# the contents, outputting to folders.

class SortRecords(luigi.contrib.spark.SparkSubmitTask):
    """
    """
    name = "Sort Records"
    app = 'spark/sort.py'

    schema_file = luigi.Parameter(default=addressbase().schema_file)
    path_root = luigi.Parameter()
    record_types = luigi.Parameter()
    key_part = luigi.Parameter('sorted-records')
    sort_field = luigi.Parameter('UPRN')

    def app_options(self):
        return [self.path_root, self.record_types, self.key_part, self.sort_field]


#------------------------------------------------------------------------------

def convert_uprn(uprn):
    return long(uprn)

def build_csv_part_path(path_root, index):
    name = 'part-{num:04d}.csv'.format(num=index)
    return join(path_root, name)

UPRN_RECORD_TYPES = ['LPI', 'DeliveryPointAddress', 'Organisation', 'Classification']
USRN_RECORD_TYPES = ['Street', 'StreetDescriptor']

class AddressbaseRecordReader(object):
    """"""
    def __init__(self, s3client, s3_path):
        self.s3client = s3client
        self.s3_path = s3_path
        self.files = [f for f in s3client.list(s3_path) if f.startswith('part')]
        # print self.files
        self.file_index = 0

    def _get_next_file_data(self):
        path = self.s3_path + '/' + self.files[self.file_index]
        print 'Downloading file: ' + path
        key = self.s3client.get_key(path)
        f = luigi.s3.ReadableS3File(key)
        self.csvdata = f.read().splitlines()
        self.file_index += 1

    def next_row(self):
        for file_part in self.files:
            self._get_next_file_data()
            csvreader = csv.reader(self.csvdata)
            for row in csvreader:
                yield row


class AddressbaseRecordQuery(AddressbaseRecordReader):
    def __init__(self, s3client, s3_path):
        super(AddressbaseRecordQuery, self).__init__(s3client, s3_path)
        self.end_uprn = -1

    def parse_csv_data(self):
        self.data = list(csv.reader(self.csvdata))
        # Get the UPRNs from the first and last lines in the CSV data and remove the decimal
        self.start_uprn = convert_uprn(self.data[0][UPRN_INDEX])
        self.end_uprn = convert_uprn(self.data[-1][UPRN_INDEX])
        self.row_index = 0
        print "start = {}, end = {}".format(self.start_uprn, self.end_uprn)

    def rows_by_uprn(self, uprn):
        while uprn > self.end_uprn:
            self._get_next_file_data()
            self.parse_csv_data()

        while self.row_index < len(self.data) and uprn < convert_uprn(self.data[self.row_index][UPRN_INDEX]):
            self.row_index += 1
        matches = []
        while self.row_index < len(self.data) and uprn == convert_uprn(self.data[self.row_index][UPRN_INDEX]):
            matches.append(self.data[self.row_index])
            self.row_index += 1
        return matches


class GroupByUPRN(luigi.Task):
    uprn_grouped_dir = luigi.Parameter(default=addressbase().uprn_grouped_dir)
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()
    host = luigi.Parameter()
    s3_path = luigi.Parameter()

    def run(self):
        s3 = luigi.s3.S3Client(self.aws_access_key_id, self.aws_secret_access_key, host=self.host)
        path = self.s3_path + '/sorted-records/Addressbase_BPLU'
        blpu_reader = AddressbaseRecordReader(s3, path)

        combine_queries = []
        for combine_type in UPRN_RECORD_TYPES:
            path = self.s3_path + '/sorted-records/Addressbase_' + combine_type
            combine_queries.append(AddressbaseRecordQuery(s3, path))

        if not os.path.exists(self.uprn_grouped_dir):
            os.makedirs(self.uprn_grouped_dir)

        count = 0
        file_count = 0
        out_path = build_csv_part_path(self.uprn_grouped_dir, file_count)
        out = open(out_path, 'w')
        csvout = csv.writer(out)
        for row in blpu_reader.next_row():
            uprn = convert_uprn(row[UPRN_INDEX])
            row[UPRN_INDEX] = uprn
            # print 'Combining UPRN: ' + str(uprn)
            csvout.writerow(row)

            # Add related rows of other record types
            for query in combine_queries:
                matches = query.rows_by_uprn(uprn)
                for m in matches:
                    csvout.writerow(m)

            count += 1
            if count % 100000 == 0:
                file_count += 1
                out.close()
                out_path = build_csv_part_path(self.uprn_grouped_dir, file_count)
                csvout = csv.writer(open(out_path, 'w'))
            # if count % 1000000 == 0:
            #     out.close()
            #     break


class GroupByUSRN(luigi.Task):
    """ Combines records with common USRN value.

    As these are smaller, it doesnt require them to be sorted initially. """

    merged_dir = luigi.Parameter(default=addressbase().merged_dir)
    usrn_grouped_dir = luigi.Parameter(default=addressbase().usrn_grouped_dir)

    def requires(self):
        tasks = []
        for record_type in USRN_RECORD_TYPES:
            dest_name = 'Addressbase_' + record_type + '.csv'
            tasks.append(AddressbaseFile(join(self.merged_dir, dest_name)))
        return tasks

    def _read_input(self, index):
        lines = self.input()[index].open('r').read().splitlines()
        data = list(csv.reader(lines))
        return sorted(data, key=operator.itemgetter(3))

    def run(self):
        streets = self._read_input(0)
        street_descriptors = self._read_input(1)

        index = 0
        file_count = 0
        out = csv.writer(open(os.path.join(build_csv_part_path(self.usrn_grouped_dir, file_count)), 'w'))
        for row in streets:
            out.writerow(row)
            while int(street_descriptors[index][USRN_INDEX]) < int(row[USRN_INDEX]):
                index += 1
            if street_descriptors[index][USRN_INDEX] == row[USRN_INDEX]:
                out.writerow(street_descriptors[index])
            index += 1
            if index % 50000 == 0:
                file_count += 1
                out = csv.writer(open(os.path.join(build_csv_part_path(self.usrn_grouped_dir, file_count)), 'w'))






