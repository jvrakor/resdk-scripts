#!/usr/bin/env python

import argparse
import csv
import glob
import os
import shutil
import tempfile
import urlparse
import requests
import resdk
import xlrd

from HTMLParser import HTMLParser
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

illumina_url = 'https://api.basespace.illumina.com'

COLUMNS = {
    'SAMPLE_NAME',
    'FILE_IDENTIFIER',
    'SEQ_TYPE',
    'COLLECTION',
    'ANNOTATOR',
    'SOURCE',
    'ORGANISM',
    'CELL_TYPE',
    'STRAIN',
    'TISSUE',
    'AGE',
    'GENOTYPE',
    'MOLECULE',
    'LIBRARY_STRATEGY',
    'EXTRACTION_PROTOCOL',
    'GROWTH_PROTOCOL',
    'TREATMENT_PROTOCOL',
    'LIBRARY_CONSTRUCTION_PROTOCOL',
    'BARCODE',
    'ANTIBODY',
    'FACILITY',
    'OTHER_CHAR_1',
    'OTHER_CHAR_2',
}

ORGANISM = {
    'Homo sapiens': 'genome-hg19',
    'Mus musculus': 'genome-mm10',
    'Dictyostelium discoideum': '',
    'Rattus norvegicus': 'genome-rn6',
    'Mus musculus/Drosophila melanogaster': 'genome-mm10-dm6',
    'Homo sapiens/Drosophila melanogaster': 'genome-hg19-dm6',
}

MOLECULE = {
    'total RNA',
    'polyA RNA',
    'cytoplasmic RNA',
    'nuclear RNA',
    'genomic DNA',
    'protein',
    'other',
}

OPTIONAL = {
    'LIBRARY_STRATEGY',
    'TISSUE',
    'AGE',
    'OTHER_CHAR_1',
    'OTHER_CHAR_2',
}


def requests_retry_session(retries=3, backoff_factor=0.3,
                           status_forcelist=(400, 404, 500, 504),
                           session=None):
    """Retry the request in the case of 400, 404, 500, or 504 status codes."""
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    return session


def get_attr_value(attrs, attr_name, default_value=''):
    """Finds attribute value or fallbacks to default value."""
    return next((value for key, value in attrs if key == attr_name), default_value)


class HiddenFieldsScraper(HTMLParser):
    """Parses HTML and extracts values of hidden inputs.

    Values are saved into a dict by field name.
    """

    def __init__(self):
        HTMLParser.__init__(self)
        self.data = {}

    def handle_starttag(self, tag, attrs):
        if tag == 'input' and get_attr_value(attrs, 'type') == 'hidden':
            field_name = get_attr_value(attrs, 'name')
            field_value = get_attr_value(attrs, 'value')

            if field_name in self.data.keys():
                raise LookupError(
                    "Key '{}' value should not be rewritten!".format(field_name)
                )

            self.data[field_name] = field_value

def scrape_hidden_fields(html):
    """Get hidden input values from HTML."""
    parser = HiddenFieldsScraper()
    parser.feed(html)

    return parser.data


class TokenObtainer(object):
    """Obtain access token for BaseSpace with given credentials."""

    def __init__(self, samples, username, password, client_id, client_secret):
        self.identifiers = []
        self.scopes = []
        self.sample_ids = {}
        self.sample_list = samples
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret

    def populate_identifiers(self):
        """Populate sample_list with data identifiers."""
        for sample in self.sample_list:
            self.identifiers.append(sample['FILE_IDENTIFIER'])

    def get_token(self, scope):
        """Get token with given permissions scope."""

        data = {
            'response_type': 'device_code',
            'client_id': self.client_id,
            'scope': scope,
        }

        # Request to authorize client_id.
        authorize_client_id = requests_retry_session().post(
            '{}/v1pre3/oauthv2/deviceauthorization'.format(illumina_url),
            data=data
        )

        # Visit returned oauth url and get redirected to a login page.
        authorization_grant = requests_retry_session().get(
            authorize_client_id.json()['verification_with_code_uri']
        )

        # The redirected url contains params that are POSTed together with login data.
        query_params = urlparse.parse_qs(urlparse.urlparse(authorization_grant.url).query)

        data = {
            'rURL': query_params['rURL'][0],
            'clientId': query_params['clientId'][0],
            'clientVars': query_params['clientVars'][0],
            'deviceType': '',
            'originPlatformUrl': 'undefined',
            'username': self.username,
            'password': self.password,
        }

        # Send login information (simulate logging in on the redirected page)
        authenticate = requests_retry_session().post(
            'https://login.illumina.com//platform-services-manager/auth/authenticate',
            json=data,
            cookies=authorization_grant.cookies
        )

        # Get permission-granting-page
        review_granting_permission = requests_retry_session().get(
            authenticate.json()['rURL'],
            cookies=authenticate.cookies
        )

        # The redirected page contains hidden inputs that are POSTed together with grant action
        hidden_fields = scrape_hidden_fields(review_granting_permission.content)

        data = {
            'useraction': 'Grant',
            'code': hidden_fields['code'],
            'workgroup_id': hidden_fields['workgroup_id'],
            '__RequestVerificationToken': hidden_fields['__RequestVerificationToken'],
        }

        # Confirm requested permissions
        grant_permissions = requests_retry_session().post(
            'https://basespace.illumina.com/oauth/device',
            data=data,
            cookies=authenticate.cookies  # Not review_granting_permission.cookies
        )

        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'code': authorize_client_id.json()['device_code'],
            'grant_type': 'device',
        }

        # Finally get token
        request_token = requests_retry_session().post(
            '{}/v1pre3/oauthv2/token'.format(illumina_url),
            data=data,
            cookies=grant_permissions.cookies
        )
        return request_token.json()

    def find_scope(self):
        """Find project ids."""
        project_id = ""
        self.populate_identifiers()
        token = self.get_token('browse global')
        token['Limit'] = 1000
        projects = requests_retry_session().get(
            '{}/v1pre3/users/current/projects'.format(illumina_url),
            params=token
        )
        for identifier in self.identifiers:
            self.sample_ids[identifier] = {}
            file_ids = []
            timestamps = []
            filenames = []
            for project in projects.json()['Response']['Items']:
                if filenames:
                    continue
                samples = requests_retry_session().get(
                    '{}/v1pre3/projects/{}/samples'.format(illumina_url, project['Id']),
                    params=token
                )
                for sample in samples.json()['Response']['Items']:
                    if filenames:
                        continue
                    reads = requests_retry_session().get(
                        '{}/v1pre3/samples/{}/files'.format(illumina_url, sample['Id']),
                        params=token
                    )
                    for read in reads.json()['Response']['Items']:
                        if identifier in read["Name"]:
                            file_ids.append(read['Id'])
                            timestamps.append(read['DateCreated'])
                            filenames.append(read['Name'])
                            if project['Id'] != project_id:
                                self.scopes.append('read project ' + project['Id'])
                                project_id = project['Id']

            if filenames:
                sort_list = zip(*sorted(zip(filenames, file_ids, timestamps)))
                filenames = list(sort_list[0])
                file_ids = list(sort_list[1])
                timestamps = list(sort_list[2])

            self.sample_ids[identifier]['file_names'] = filenames
            self.sample_ids[identifier]['file_ids'] = file_ids
            self.sample_ids[identifier]['timestamps'] = timestamps

    def get_read_token(self):
        """Get the ultimate token."""
        self.find_scope()
        scope = ', '.join(self.scopes)

        return self.get_token(scope), self.sample_ids


class CombinedFile(object):
    """Combine and identify files."""

    def __init__(self, token, file_ids, filenames):
        self.token = token
        self.file_ids = file_ids
        self.filenames = filenames

    def find_and_get_combined_file(self, tmp_dir, paired):
        """Concatenate files."""
        temp_file_path = ""
        for filename, file_id in zip(self.filenames, self.file_ids):
            if paired in filename:
                name = filename.split('_')[0] # This contains the file identifiers
                if not temp_file_path:
                    print('Tempfile is being created...')
                    print('Downloading and concatenating file(s):')
                    temp_file_path = tmp_dir + '/' + name + '_' + paired + '.fastq.gz'
                print(filename)
                data = requests_retry_session().get(
                    '{}/v1pre3/files/{}/content'.format(illumina_url, file_id),
                    params=self.token
                )
                with open(temp_file_path, 'ab') as temp_file:
                    for chunk in data.iter_content(chunk_size=1024):
                        temp_file.write(chunk)

        return temp_file_path


class Sample(object):
    """Create a Sample like object."""

    def __init__(self, sample):
        self.name = sample['SAMPLE_NAME']
        self.identifier = sample['FILE_IDENTIFIER']
        self.collection = sample['COLLECTION']
        self.reads_annotation = {
            'experiment_type': sample['SEQ_TYPE'],
            'protocols': {
                'extract_protocol': sample['EXTRACTION_PROTOCOL'],
                'library_prep': sample['LIBRARY_CONSTRUCTION_PROTOCOL'],
                'treatment_protocol': sample['TREATMENT_PROTOCOL'],
                'growth_protocol': sample['GROWTH_PROTOCOL'],
                'antibody_information': {
                    'manufacturer' : sample['ANTIBODY']
                }
            },
            'reads_info': {
                'barcode': sample['BARCODE'],
                'facility': sample['FACILITY']
            }
        }
        self.molecule = sample['MOLECULE']
        self.organism = sample['ORGANISM']
        self.sample_annotation = {
            'sample': {
                'annotator': sample['ANNOTATOR'],
                'cell_type': sample['CELL_TYPE'],
                'source': sample['SOURCE'],
                'strain': sample['STRAIN'],
                'genotype': sample['GENOTYPE'],
                'optional_char': []
            }
        }
        if self.organism:
            self.sample_annotation['sample']['organism'] = self.organism.split('/')[0]
        if self.molecule:
            self.sample_annotation['sample']['molecule'] = self.molecule
        for option in OPTIONAL:
            if sample[option]:
                self.sample_annotation['sample']['optional_char'].append(
                    '{0}:{1}'.format(option, sample[option]))

    def tag_community(self):
        """Prepare community tags."""
        seq = self.reads_annotation['experiment_type'].lower()
        if 'rna' in seq:
            community = 'community:rna-seq'
        elif 'chip' in seq:
            community = 'community:chip-seq'
        else:
            community = None

        return community


class FileImporter(object):
    """Import annotation spreadsheet."""

    def __init__(self, annotation_path):
        self.sample_list = []
        self.path = annotation_path

    def extension(self):
        """Find spreadsheet file extension."""
        return os.path.splitext(self.path)[1]

    def _read_xlrd(self):
        """Read Excel spreadsheet annotation file."""
        workbook = xlrd.open_workbook(self.path)
        worksheet = workbook.sheets()[0]
        header = worksheet.row_values(0)
        for rownum in range(1, worksheet.nrows):
            row = worksheet.row_values(rownum)
            entries = {}
            for i, value in enumerate(row):
                if isinstance(value, float):
                    entries[header[i]] = str(value)
                else:
                    entries[header[i]] = value

            self.sample_list.append(entries)

    def _read_text_file(self):
        """Read simple spreadsheet annotation file."""
        with open(self.path, 'rb') as sample_sheet:
            self.sample_list = list(csv.DictReader(sample_sheet, delimiter='\t'))

    def populate_samples(self):
        """Check the format of annotation file and asign read function."""
        if self.extension() in ['.xls', '.xlsx', '.xlsm']:
            self._read_xlrd()
        elif self.extension() in ['.txt', '.tab', '.tsv']:
            self._read_text_file()
        else:
            raise TypeError(
                "Annotation spreadsheet extension `{}` not recognised. Options"
                " are: `.xls`, `.xlsx`, `.xlsm`, `.txt`, `.tab`, `.tsv`.".format(self.extension())
            )

    def validate(self):
        """Validate the annotation spreadsheet file."""
        for sample in self.sample_list:
            diff1 = COLUMNS - set(sample.keys())
            diff2 = set(sample.keys()) - COLUMNS
            for var_name, VAR in [('organism', ORGANISM), ('molecule', MOLECULE)]:
                var = sample[var_name.upper()]
                if var and var not in VAR:
                    raise ValueError(
                        "`{0}` is not a valid {1}. Valid"
                        " {2}s are found in Annotation_spreadsheet"
                        ".xlsm sheet Options. Spreadsheet can be found on "
                        "https://github.com/genialis/resdk-scripts/tree/master"
                        "/BCM_project/upload_scripts.".format(var, var_name.upper(), var_name)
                    )

            err_msg = (
                "Headers `{0}` {1}. You should use the"
                " headers from Annotation_spreadsheet.xlsm found on "
                "https://github.com/genialis/resdk-scripts/tree/master/"
                "BCM_project/upload_scripts."
            )
            if diff1:
                raise NameError(
                    err_msg.format(', '.join(diff1), "are missing")
                    )
            if diff2:
                raise NameError(
                    err_msg.format(', '.join(diff2), "not recognised")
                    )


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Upload raw data.')
    parser.add_argument('--sample_sheet', type=str, help='Sample sheet', required=True)
    parser.add_argument('--username_genialis', type=str, help='Genialis username', required=True)
    parser.add_argument('--password_genialis', type=str, help='Genialis password', required=True)
    parser.add_argument('--URL', type=str, help='URL', required=True)
    parser.add_argument('--temp_root',
                        type=str,
                        help='Where only the working node can access the created files',
                        required=True)
    parser.add_argument('--shared_temp_root', type=str,
                        help='Where all nodes can access the created files', required=True)
    parser.add_argument('--username_basespace', type=str,
                        help='Illumina Basespace username', required=True)
    parser.add_argument('--password_basespace', type=str,
                        help='Illumina Basespace password', required=True)
    parser.add_argument('--client_id', type=str,
                        help='Illumina Basespace App Client Id', required=True)
    parser.add_argument('--client_secret', type=str,
                        help='Illumina Basespace App Client Secret', required=True)
    parser.add_argument('--bowtie2', action='store_true', help='Run Bowtie2 on uploaded data')
    parser.add_argument('--force', action='store_true', help='Force upload')
    parser.add_argument('--keep', action='store_true', help='Keep concatenated files')

    return parser.parse_args()


def get_or_create_collection(resolwe, coll_name):
    """Check if Collection with given name already exists. Create new Collection if not."""

    collections = resolwe.collection.filter(name=coll_name)
    if len(collections) > 1:
        raise LookupError(
            "More than one collection with name '{}' already exists on the platform!"
            "".format(coll_name)
        )

    if not collections:
        collection = resdk.resources.Collection(resolwe=resolwe)
        collection.name = coll_name
        collection.save()
    else:
        collection = resolwe.collection.get(name=coll_name)

    return collection


def upload(resolwe, src1, src2):
    """Upload reads to a selected URL and Collection."""
    if src2:
        uploaded_reads = resolwe.run('upload-fastq-paired',
                                     input={
                                         'src1': src1,
                                         'src2': src2,
                                     })
    else:
        uploaded_reads = resolwe.run('upload-fastq-single',
                                     input={'src': src1})

    return uploaded_reads


def main():
    args = parse_arguments()

    res = resdk.Resolwe(args.username_genialis, args.password_genialis, args.URL)
    resdk.start_logging()

    # Read  and validate the annotation template
    annotation = FileImporter(args.sample_sheet)
    annotation.populate_samples()
    annotation.validate()

    empty_token = TokenObtainer(annotation.sample_list, args.username_basespace,
                                args.password_basespace, args.client_id, args.client_secret)
    token, ids = empty_token.get_read_token()

    for sample in annotation.sample_list:
        read_file = Sample(sample)

        if not ids[read_file.identifier]['file_names']:
            print("Warning: Data with identifier {} was not found on Basespace."
                  " Please check the annotation spreadsheet if the identifier"
                  " string is correct.".format(read_file.identifier))
            continue

        # Log and break if the script is running with the same spreadsheet as input
        filepath = args.shared_temp_root + args.sample_sheet
        if glob.glob(filepath):
            raise RuntimeError(
                "A script with the same annotation spreadsheet ('{}') as input is already running"
                " at the moment!".format(args.sample_sheet))

        open(filepath, "w+").close()

        empty_file = CombinedFile(token, ids[read_file.identifier]['file_ids'],
                                  ids[read_file.identifier]['file_names'])

        # Check if data is already on the platform
        ids_timestamps = zip(
            ids[read_file.identifier]['file_ids'],
            ids[read_file.identifier]['timestamps']
        )

        tag = ["id:{0};timestamp:{1}".format(i, j) for i, j in ids_timestamps]
        tag.append("len:{}".format(len(tag)))

        if not args.force:
            online_data = res.data.filter(tags=tag)
            if online_data:
                print(
                    "Sample `{0}` data is already on the platform: {1}"
                    "".format(read_file.name, online_data)
                )
                continue

        # Concatenate files
        try:
            if not os.path.isdir(tempdir):
                raise NameError
        except NameError:
            tempdir = tempfile.mkdtemp(dir=args.temp_root)

        fpath1 = empty_file.find_and_get_combined_file(tempdir, 'R1')
        fpath2 = empty_file.find_and_get_combined_file(tempdir, 'R2')

        # Make/get a collection
        if read_file.collection:
            coll = get_or_create_collection(res, read_file.collection)
        else:
            coll = None

        # Upload data
        try:
            reads = upload(res, fpath1, fpath2)
        except:
            os.remove(filepath)
            raise RuntimeError(
                "Error occured while uploading sample `{}`. Try again.".format(read_file.name)
            )

        # Attach unique identifiers to reads and remove concatenated files
        reads.tags = tag
        if not args.keep:
            shutil.rmtree(tempdir)

        # Provide reads annotation
        reads.descriptor_schema = 'reads'
        reads.descriptor = read_file.reads_annotation
        reads.save()

        # Get sample object
        main_sample = reads.sample

        # Rename the sample
        main_sample.name = read_file.name
        main_sample.slug = read_file.name

        # Provide sample annotation
        main_sample.descriptor_schema = 'sample'
        main_sample.descriptor = read_file.sample_annotation

        # Tag sample community
        if read_file.tag_community():
            main_sample.tags.append(read_file.tag_community())

        main_sample.save()

        # Confirm that/if the sample is annotated
        if read_file.organism and read_file.molecule:
            main_sample.confirm_is_annotated()

        # Run bowtie2 and bam-split (if neccessary) on the sample
        if read_file.organism in ORGANISM and args.bowtie2:
            genome = res.data.get(ORGANISM[read_file.organism])
            bam = main_sample.run_bowtie2(genome)

            # Split hybrid bam
            if '/' in read_file.organism:
                build = ORGANISM[read_file.organism].split('-')
                inputs = {
                    'bam': bam[0].id,
                    'organism': build[1],
                    'organism2': build[2],
                }
                res.run(slug='bam-split', input=inputs)

        # Update the sample
        main_sample.update()

        # Attach sample and sample data to the collection
        if coll:
            coll.add_samples(main_sample)
            for data in main_sample.data:
                coll.add_data(data)

    # Clean log file
    os.remove(filepath)


    print('Finished')


if __name__ == "__main__":
    main()
