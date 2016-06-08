import luigi
import os
from os import listdir
from os.path import isfile, join
import yaml


RESULTS_COUNT_DIR = 'results/count/'


class AddressbaseDataTask(luigi.Task):

    # Example parameter for our task: a
    # date for which a report should be run
    directory = luigi.Parameter()

    # def requires(self):
    #     """
    #     Which other Tasks need to be complete before
    #     this Task can start? Luigi will use this to
    #     compute the task dependency graph.
    #     """
    #     # return [MyUpstreamTask(self.report_date)]

    def output(self):
        """
        When this Task is complete, where will it produce output?
        Luigi will check whether this output (specified as a Target)
        exists to determine whether the Task needs to run at all.
        """
        return luigi.LocalTarget('results/list.txt')

    def run(self):
        """
        How do I run this Task?
        Luigi will call this method if the Task needs to be run.
        """
        onlyfiles = [f for f in listdir(self.directory) if isfile(join(self.directory, f)) and f.endswith('.csv')]
        f = self.output().open('w')
        # f.writelines(onlyfiles)
        [f.write(p + "\n") for p in onlyfiles]
        f.close()
        # for p in onlyfiles:
        #     print(p)
        # f.close()


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
        # if self.file_in.endswith('.csv'):
        #     return luigi.LocalTarget(self.file_in)
        # else:
        #     return luigi.LocalTarget(self.file_in, format=luigi.format.Gzip)

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
            config= {}
            config['name'] = cf[:-4]
            config['counts'] = counts
            manifest.append(config)
        with self.output().open('w') as out:
            out.write(yaml.dump(manifest, default_flow_style=False))


class SplitRecordsTask(luigi.Task):
    directory = luigi.Parameter()
    dest = luigi.Parameter()
    config = luigi.DictParameter()
    # counts = luigi.Parameter()

    def requires(self):
        return CountAllRecordsTask(self.directory)

    def output(self):
        # filename = os.path.basename(self.file_in)
        return luigi.LocalTarget(join(self.dest, self.config['name']))

    def run(self):
        file_in = join(self.directory, self.config['name'])
        lines = open(file_in).readlines()
        # Remove first and last two lines, as they are different types
        lines = lines[1:-2]
        headers = ','.join(self.config['schema']['schema'])
        with self.output().open('w') as out:
            out.write(headers + "\n")
            out.writelines(lines)


class SplitAllRecordsTask(luigi.Task):
    directory = luigi.Parameter()
    schema_file = luigi.Parameter()
    location_record_types = luigi.Parameter()

    def requires(self):
        return CountAllRecordsTask(self.directory)

    # def output(self):
    #     pass

    def run(self):
        manifest = yaml.load(self.input().open('r'))
        schema = yaml.load(open(self.schema_file, 'r'))
        for config in manifest:
            # Remove unwanted counts that are not important
            del config['counts']['10']
            del config['counts']['29']
            del config['counts']['99']
            for k in config['counts'].iterkeys():
                config['schema'] = schema[k]
                yield SplitRecordsTask(directory=self.directory, config=config)



# class SplitRecordsTask(luigi.Task):
#     directory = luigi.Parameter()

#     def requires(self):

#     def output(self):

#     def run(self):





