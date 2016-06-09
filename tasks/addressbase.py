import logging
import luigi
import os
from os import listdir
from os.path import isfile, join
import yaml


RESULTS_COUNT_DIR = 'results/count/'


class addressbase(luigi.Config):
    directory = luigi.Parameter()
    schema_file = luigi.Parameter()
    location_records_filter = luigi.Parameter()
    include_headers = luigi.BoolParameter(default=False)


""" A source Addressbase file, either a CSV or ZIP file.

It is assumed to already exist.
"""
class AddressbaseFile(luigi.ExternalTask):
    file_in = luigi.Parameter()

    def output(self):
        if self.file_in.endswith('.csv'):
            return luigi.LocalTarget(self.file_in)
        else:
            return luigi.LocalTarget(self.file_in, format=luigi.format.Gzip)


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


""" Creates a manifest of the Addressbase files.

Lists all the files and how many record types are in each one.
"""
class CountAllRecordsTask(luigi.Task):
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



class MergeRecordsTask(luigi.Task):
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

            # config['schema'] = schema[record_id]
            # config['dest_name'] = 'Addressbase_' + config['schema']['name'] + '.csv'
            # yield MergeRecordsTask(config=config)

        # for config in manifest:
        #     # Remove unwanted counts that are not important
        #     for num in ['10', '29', '99']:
        #         del config['counts'][num]

        #     # Most files have 1 key, a few have 2
        #     for record_id in config['counts'].iterkeys():
        #         # Only process the record types that are needed
        #         if record_id in records_filter:
        #             config['schema'] = schema[record_id]
        #             config['dest_name'] = 'Addressbase_' + config['schema']['name'] + '.csv'
        #             yield MergeRecordsTask(config=config)



# class SplitRecordsTask(luigi.Task):
#     directory = luigi.Parameter()

#     def requires(self):

#     def output(self):

#     def run(self):





