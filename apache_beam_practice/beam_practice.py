import apache_beam as beam


class SplitRow(beam.DoFn):
    def process(self, element):
        return [element.split(',')]


class FilterAccountsEmployee(beam.DoFn):
    def process(self, element):
        if element[3] == 'Accounts':
            return [element]


class PairEmployee(beam.DoFn):
    def process(self, element):
        return [(element[1]+","+element[3], 1)]


class Counting(beam.DoFn):
    def process(self, element):
        (key, value) = element
        return [(key, sum(value))]


class MyTransform(beam.PTransform):

    def expand(self, input_col1):
        a = (
            input_col1
                        |'Group and sum' >> beam.CombinePerKey(sum)
                        |'count filter account' >> beam.Filter(filter_on_count)
                        |'Regular account Employee' >> beam.Map(format_output)
        )
        return a


def split_row(element):     # correct
    return element.split(',')


def filtering(record):      # correct
    return record[3] == 'Accounts'


def filtering_hr(record):      # correct
    return record[3] == 'HR'


def filter_on_count(element):   # correct
    name, count = element
    if count > 30:
        return element


def format_output(element):
    name, count = element
    return ', '.join((name.encode('ascii'),str(count),'Regular Employee'))


def practice_pipeline_1(test=True):  # practice setting up a pipeline
    p1 = beam.Pipeline()
    if test:
        attendance_count = (
            p1
            | beam.io.ReadFromText('dept_data.txt')
            | beam.Map(lambda record: record.split(','))
            | beam.io.WriteToText('practice_1')
        )
        p1.run()
        return None
    else:
        attendance_count = (
                p1
                | beam.io.ReadFromText('dept_data.txt')
                | beam.Map(lambda record: record.split(','))
        )
        p1.run()
        return attendance_count


def practice_pipeline_2(test=True):  #setup a pipeline and split elements, pipeline reads data as text not rows and columns.
    p2 = beam.Pipeline()
    attendance_count = (
        p2
        | beam.io.ReadFromText('dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.Map(lambda record: (record[1], 1))
        | beam.io.WriteToText('practice_2')
    )
    p2.run()
    return None


def practice_pipeline_3():  # setup pipeline, split elements and count occurrence of keys.
    p3 = beam.Pipeline()
    attendance_count = (
        p3
        | beam.io.ReadFromText('dept_data.txt')
        | beam.Map(lambda record: record.split(','))
        | beam.Map(lambda record: (record[1], 1))

        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('practice_3')
    )
    p3.run()
    return None


def practice_pipeline_4():  # correct
    p4 = beam.Pipeline()
    account_count = (
        p4
        | 'Read file' >> beam.io.ReadFromText('dept_data.txt')
        | 'Transform file' >> beam.Map(split_row)

        | 'filter accounts team' >> beam.Filter(filtering)
        | 'take key, value pairs' >> beam.Map(lambda record: (record[1], 1))
        | beam.CombinePerKey(sum)
        | beam.io.WriteToText('practice_4')

    )
    p4.run()


def practice_pipeline_5(): # correct

    p5 = beam.Pipeline()
    p5_input = (
            p5
            | beam.io.ReadFromText('dept_data.txt')
            | beam.Map(lambda record: record.split(','))
    )
    account_count = (
        p5_input
        |'Get all account department people' >> beam.Filter(filtering)
        |'Pair each accounts employee with int 1' >> beam.Map(lambda record: ('Accounts, '+record[1], 1))
        |'Group and sum ACCOUNTS' >> beam.CombinePerKey(sum)

    )

    hr_count = (
        p5_input
        |'Get all HR department people' >> beam.Filter(lambda record: record[3] == 'HR')
        |'Pair each HR employee with int 1' >> beam.Map(lambda record: ('HR, '+record[1], 1))
        |'Group and sum HR' >> beam.CombinePerKey(sum)

    )
    output = (
        (account_count, hr_count)
        | beam.Flatten()
        | beam.io.WriteToText('practice_5')
    )
    p5.run()


def practice_pipeline_6():  # needs fixing
    p6 = beam.Pipeline()

    attendance_count = (
        p6
        |beam.io.ReadFromText('dept_data.txt')

        |beam.ParDo(SplitRow())

        |'Group' >> beam.GroupByKey()
        |'Sum using Pardo' >> beam.ParDo(Counting())

        |beam.io.WriteToText('practice_6')
    )
    p6.run()


def practice_pipeline_7():
    p7 = beam.Pipeline()
    input_collection = (
        p7
        |'Read File' >> beam.io.ReadFromText('dept_data.txt')
        |'Split row' >> beam.Map(split_row)
    )

    account_count = (
        input_collection
        | 'Get all accounts dept person' >> beam.Filter(filtering)
        | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ('Accounts, '+record[1], 1))
        | 'composite accounts' >> MyTransform()
        | 'Write result for account' >> beam.io.WriteToText('practice_7_accounts')
    )

    hr_count = (
            input_collection
            | 'Get all hr dept person' >> beam.Filter(filtering_hr)
            | 'Pair each hr employee with 1' >> beam.Map(lambda record: ('HR, ' + record[1], 1))
            | 'composite hr' >> MyTransform()
            | 'Write result for hr' >> beam.io.WriteToText('practice_7_hr')
    )
    p7.run()


practice_pipeline_6()





