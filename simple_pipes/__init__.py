%matplotlib inline

import csv
import copy
from itertools import groupby
from collections import namedtuple, Iterable
from operator import itemgetter

# 4 big cases we need in this project: transform, filter, aggregate, and split

class ETLStreamingStep():
    # Builds a transform step where you pass a function at construction which defines the transform

    def __init__(self, row_transform=None):
        self.step = row_transform


    def run(self, data):
        for row in data:
            yield self.step(row)


    def _step(self, row):
        if self.step:
            return self.step(row)

        return row


class ETLAggStep():
    # Builds an aggregation step where you pass a key function and a dict of fields to add
    # Output willl have all fields in key func + any new fields defined in the dict
    # Values in added fields dict should be functions that take in the grouped rows as only input
    #
    # This is a major source of slowness, as it uses python's groupby() which needs the whole array
    # in memory

    def __init__(self, key_func, add_fields={}):

        self._key_func = key_func
        if isinstance(key_func, Iterable):
            self._key_func = self._get_grouping_key(key_func)

        self._add_fields = add_fields


    def run(self, data):
        d = sorted(list(data), key=self._key_func)

        for key, group in groupby(d, key=self._key_func):
            group=list(group)

            agg_row = key._asdict()

            if callable(self._add_fields):
                agg_row.update(self._add_fields(group))

            elif isinstance(self._add_fields, dict):
                agg_row.update({
                    key: func(group) for key, func in self._add_fields.items()
                })

            yield agg_row


    def _get_grouping_key(self, fields):

        key = namedtuple('GroupingKey', fields)

        def get_key(row):
            return key(**{field: row[field] for field in fields})

        return get_key


class ETLSplitStep():

    def __init__(self, row_splitter=None):
        self.step = row_splitter or self._step


    def run(self, data):

        for row in data:
            for split in self.step(row):
                yield split


    def _step(self, row):
        return [row]


class ETLFilterStep():

    def __init__(self, filter_func):
        self._filter_func = filter_func


    def run(self, data):
        return filter(self._filter_func, data)


class ETLPipeline():

    def __init__(self, source, steps=[]):
        self._source = source
        self._steps = steps


    def run(self):
        self._data = self._source

        for step in self._steps:
            self._data = step.run(self._data)

        return self._data


def read_csv(file):

    with open(file, 'r') as fp:
        for row in csv.DictReader(fp):
            yield row


def compound_key_func_gen(fields, inv_transforms={}):
    def key_func(row):
        return '-'.join([str(row[field]) for field in fields])

    def inverse_key_func(key):
        row = dict(zip(fields, key.split('-')))
        row.update({
            key: func(row[key]) for key, func in inv_transforms.items()
        })

        return row

    return key_func, inverse_key_func


def get_value_normalizer(value_norms):
    # This is a bit ugly, but it reads nicely once you get what is going on.
    # Basically, you put in a big mapping of normalized values for fields as a 2 or 3-level dict
    # So, for each field listed, it sees if the value is any of the 2nd level key, if so
    # it returns the final value in case of a dict, or processes the value with a function.
    #
    # eg: {'race': {
    #              'gov./lieutenant gov.': 'gov',
    #              'governor/lieutenant governor': 'gov',
    #              'attorney general': 'ag'
    #              }
    #     }
    #
    # This would look in the column 'race' and then map two different ways the data is
    # represented across different years on to the common value 'gov'
    #
    # Returns the function which performs this action to fit into the pipeline style

    def norm_values(row):
        output = {}

        for key, val in row.items():
            norm = value_norms.get(key, {})
            if callable(norm):
                val = norm(val)

            elif isinstance(norm, dict):
                val = norm.get(val, val)

            else:
                raise Exception('Unknown normalizer for "{}"'.format(key))

            output[key] = val

        return output

    return norm_values


def get_field_normalizer(field_norms):
    # This does a very similar process to get_value_normalizer, but on the column names themselves

    def norm_fields(row):
        output = {}

        for key, val in row.items():
            if callable(field_norms):
                key = field_norms(key)

            elif isinstance(field_norms, dict):
                key = field_norms.get(key, key)

            else:
                raise Exception('Unknown field normalizer')

            if key is not None:
                output[key] = val

        return output

    return norm_fields


def merge_fields(group):
    output = {}

    for row in group:
        for key, val in row.items():
            if key not in output:
                output[key] = val

            elif output[key] != val:
                raise Exception('Unable to merge because conflicting values for {} ({} != {})'.format(key, val, output[key]))

    return output


def sum_by_field(field):

    return  lambda group: sum(map(lambda row: row[field], group))


def pivot_on(pivot_field, value_field):

    def pivot(group):
        output = {}
        for row in group:
            key = row[pivot_field]

            if key in output:
                raise Exception('Collision in pivot for field {}'.format(key))

            output[key] = row[value_field]

        return output

    return pivot


def add_fields(fields):

    def field_adder(row):
        row = copy.deepcopy(row)
        for key, func in fields.items():
            row.update({key: func(row)})

        return row

    return field_adder
