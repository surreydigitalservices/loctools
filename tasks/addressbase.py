import csv
import logging
import luigi
import luigi.s3
import operator
import os
from os import listdir
from os.path import isfile, join
import yaml


USRN_INDEX = 3
UPRN_INDEX = 3
RESULTS_COUNT_DIR = 'results/count/'


class addressbase(luigi.Config):
    directory = luigi.Parameter()
    merge_dest = luigi.Parameter()
    schema_file = luigi.Parameter()
    location_records_filter = luigi.Parameter()
    include_headers = luigi.BoolParameter(default=False)


class AddressbaseFile(luigi.ExternalTask):
    """ A source Addressbase file, either a CSV or ZIP file.

    It is assumed to already exist.
    """

    file_in = luigi.Parameter()

    def output(self):
        if self.file_in.endswith('.csv'):
            return luigi.LocalTarget(self.file_in)
        else:
            return luigi.LocalTarget(self.file_in, format=luigi.format.Gzip)


#------------------------------------------------------------------------------

class CountRecordsTask(luigi.Task):
    file_in = luigi.Parameter()

    def requires(self):
        return AddressbaseFile(self.file_in)

    def output(self):
        filename = os.path.basename(self.file_in) + '.yml'
        return luigi.LocalTarget(join(RESULTS_COUNT_DIR, filename))

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


class CountAllRecordsTask(luigi.Task):
    """ Creates a manifest of the Addressbase files.

    Lists all the files and how many record types are in each one.
    """

    directory = luigi.Parameter()

    def requires(self):
        csvfiles = [f for f in listdir(self.directory) if f.endswith('.csv')]
        return [CountRecordsTask(file_in=join(self.directory, f)) for f in csvfiles]

    def output(self):
        return luigi.LocalTarget(join(self.directory, 'manifest.yml'))

    def run(self):
        countfiles = [f for f in listdir(RESULTS_COUNT_DIR) if f.endswith('.yml')]
        manifest = []

        for cf in countfiles:
            counts = yaml.load(open(join(RESULTS_COUNT_DIR, cf)))
            config = {'name': cf[:-4], 'counts': counts}
            manifest.append(config)

        with self.output().open('w') as out:
            out.write(yaml.dump(manifest, default_flow_style=False))


#------------------------------------------------------------------------------

class SplitRecordsTask(luigi.Task):
    directory = luigi.Parameter()
    dest = luigi.Parameter()
    config = luigi.DictParameter()
    include_headers = luigi.BoolParameter(default=False)

    def requires(self):
        return CountAllRecordsTask(self.directory)

    def output(self):
        return luigi.LocalTarget(join(self.dest, self.config['dest_name']))

    def run(self):
        file_in = join(self.directory, self.config['name'])
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


class SplitAllRecordsTask(luigi.Task):
    directory = luigi.Parameter(default=addressbase().directory)
    schema_file = luigi.Parameter()
    location_records_filter = luigi.Parameter()

    def requires(self):
        return CountAllRecordsTask(self.directory)

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
                    yield SplitRecordsTask(directory=self.directory, config=config)


#------------------------------------------------------------------------------

class MergeRecordsTask(luigi.Task):
    """ Merges records of a single type. """

    schema_file = luigi.Parameter(default=addressbase().schema_file)
    directory = luigi.Parameter(default=addressbase().directory)
    dest = luigi.Parameter()
    # config = luigi.DictParameter()
    record_id = luigi.Parameter()
    include_headers = luigi.BoolParameter(default=False)
    addressbase_inputs = []

    def requires(self):
        return CountAllRecordsTask(self.directory)

    def output(self):
        manifest = yaml.load(self.input().open('r'))
        schema_data = yaml.load(open(self.schema_file, 'r'))
        self.record_def = schema_data[self.record_id]
        dest_name = 'Addressbase_' + self.record_def['name'] + '.csv'
        return luigi.LocalTarget(join(self.dest, dest_name))

    def create_inputs(self):
        manifest = yaml.load(self.input().open('r'))
        inputs = []
        for config in manifest:
            # Remove unwanted counts that are not important
            for num in ['10', '29', '99']:
                del config['counts'][num]
            if str(self.record_id) in config['counts'].keys():
                # file_target = AddressbaseFile(file_in=join(self.directory, config['name']))
                file_target = join(self.directory, config['name'])
                # print file_target
                inputs.append({'path': file_target, 'mixed': config['counts'].keys() > 1})
        return inputs

    def create_combined_output(self):
        with self.output().open('w') as out:
            if self.include_headers:
                headers = ','.join(self.record_def['schema'])
                out.write(headers + "\n")

            for file_in in self.addressbase_inputs:
                # logging.debug('Reading input file: ' + file_in)
                print('Reading input file: ' + file_in['path'])
                lines = open(file_in['path'], 'r').readlines()
                # Remove first and last two lines, as they are unused records
                lines = lines[1:-2]
                # TODO: improve speed
                if file_in['mixed']:
                    for l in lines:
                        if l.startswith(str(self.record_id)):
                            out.write(l)
                else:
                    out.writelines(lines)
                out.flush()

    def run(self):
        self.addressbase_inputs = self.create_inputs()
        # for t in self.inpuf_defs:
            # yield must occur inside run()!
            # target = yield AddressbaseFile(file_in=t)
            # self.addressbase_inputs.append(target)
        self.create_combined_output()


class MergeAllRecordsTask(luigi.Task):
    directory = luigi.Parameter(default=addressbase().directory)
    schema_file = luigi.Parameter(default=addressbase().schema_file)
    location_records_filter = luigi.Parameter(default=addressbase().location_records_filter)

    def requires(self):
        return CountAllRecordsTask(self.directory)

    def run(self):
        manifest = yaml.load(self.input().open('r'))
        schema = yaml.load(open(self.schema_file, 'r'))
        records_filter = self.location_records_filter.split(',')

        for record_id in records_filter:
            yield MergeRecordsTask(record_id=record_id)


#------------------------------------------------------------------------------

class AddressbaseRecordReader(object):
    """"""

    def __init__(self, s3client, s3_path):
        self.s3client = s3client
        self.s3_path = s3_path
        self.files = [f for f in s3client.list(s3_path) if f.startswith('part')]
        # print self.files
        self.index = 0
        self._get_next_file_data()
        # self.csvdata =

    def _get_next_file_data(self):
        path = self.s3_path + '/' + self.files[self.index]
        print 'Fetching next file: ' + path
        key = self.s3client.get_key(path)
        f = luigi.s3.ReadableS3File(key)
        self.csvdata = f.read().splitlines()
        self.index += 1

    def next_row(self):
        csvreader = csv.reader(self.csvdata)
        for row in csvreader:
            yield row

def convert_uprn(uprn):
    return long(float(uprn))

class AddressbaseRecordQuery(AddressbaseRecordReader):
    def __init__(self, s3client, s3_path):
        super(AddressbaseRecordQuery, self).__init__(s3client, s3_path)
        self.parse_csv_data()

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


TYPES_TO_COMBINE = ['LPI', 'DeliveryPointAddress', 'Organisation', 'Classification']

class CombineByUPRNTask(luigi.Task):
    directory = luigi.Parameter(default=addressbase().directory)
    schema_file = luigi.Parameter(default=addressbase().schema_file)
    location_records_filter = luigi.Parameter(default=addressbase().location_records_filter)
    aws_access_key_id = luigi.Parameter()
    aws_secret_access_key = luigi.Parameter()
    host = luigi.Parameter()
    s3_path = luigi.Parameter()

    def run(self):
        s3 = luigi.s3.S3Client(self.aws_access_key_id, self.aws_secret_access_key, host=self.host)
        path = self.s3_path + '/sorted-records/Addressbase_BPLU'
        blpu_reader = AddressbaseRecordReader(s3, path)

        combine_queries = []
        for combine_type in TYPES_TO_COMBINE:
            path = self.s3_path + '/sorted-records/Addressbase_' + combine_type
            combine_queries.append(AddressbaseRecordQuery(s3, path))

        merge_dir = 'cache/merged_records'
        if not os.path.exists(merge_dir):
            os.makedirs(merge_dir)
        out = open('cache/merged_records/0000.csv', 'w')
        csvout = csv.writer(out)
        count = 0
        for row in blpu_reader.next_row():
            uprn = convert_uprn(row[UPRN_INDEX])
            row[UPRN_INDEX] = uprn
            # print 'Combining UPRN: ' + str(uprn)
            csvout.writerow(row)

            # Add related rows of other record types
            for query in combine_queries:
                matches = query.rows_by_uprn(uprn)
                for m in matches:
                    m[UPRN_INDEX] = convert_uprn(m[UPRN_INDEX])
                    csvout.writerow(m)

            count += 1
            if count > 1000000:
                out.close()
                break


class CombineByUSRNTask(luigi.Task):
    USRN_RECORD_TYPES = ['Street', 'StreetDescriptor']

    directory = luigi.Parameter(default=addressbase().directory)
    merge_dest = luigi.Parameter(default=addressbase().merge_dest)

    def requires(self):
        tasks = []
        for record_type in CombineByUSRNTask.USRN_RECORD_TYPES:
            dest_name = 'Addressbase_' + record_type + '.csv'
            tasks.append(AddressbaseFile(join(self.merge_dest, dest_name)))
        return tasks

    def read_input(self, index):
        lines = self.input()[index].open('r').read().splitlines()
        data = list(csv.reader(lines))
        return sorted(data, key=operator.itemgetter(3))

    def run(self):
        streets = self.read_input(0)
        street_descriptors = self.read_input(1)

        index = 0
        file_count = 0
        out = csv.writer(open(os.path.join('cache/grouped_records', '0000.csv'), 'w'))
        for row in streets:
            out.writerow(row)
            while int(street_descriptors[index][USRN_INDEX]) < int(row[USRN_INDEX]):
                index += 1
            if street_descriptors[index][USRN_INDEX] == row[USRN_INDEX]:
                out.writerow(street_descriptors[index])
            index += 1
            if index % 50000 == 0:
                file_count += 1
                out = csv.writer(open(os.path.join('cache/grouped_records', "000{}.csv".format(file_count)), 'w'))






# class SplitRecordsTask(luigi.Task):
#     directory = luigi.Parameter()

#     def requires(self):

#     def output(self):

#     def run(self):





