from typing import List
from datetime import datetime

import apache_beam as beam


def format_output(sum_pair):
    key_name, miss_month = sum_pair
    return str(key_name) + ', ' + str(miss_month) + ' missed'   # 'CT28383, Miyako, Burns, [1 or 0] missed'


def format_result(sum_pair) -> str:
    key_name, points = sum_pair
    return str(key_name) + ', ' + str(points) + ' fraud points'   # 'CT28383, Miyako, Burns, 2 fraud points'


def calculate_month(input_list):  # CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018
    # convert payment date to datetime and extract month of payment

    payment_date = datetime.strptime(input_list[8].rstrip().lstrip(), '%d-%m-%Y')
    input_list.append(str(payment_date.month))

    return input_list


def calculate_late_payment(element: List) -> List:
    # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018]

    due_date = datetime.strptime(element[6].rstrip().lstrip(), '%d-%m-%Y')      # due_date 01-12-2020
    payment_date = datetime.strptime(element[8].rstrip().lstrip(), '%d-%m-%Y')

    if payment_date <= due_date:
        element.append('0')
    else:
        element.append('1')

    return element          # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018,1]


def calculate_points_cards(element):
    # CT28383,Miyako,Burns,R_7488,Issuers,500,490,38,101,30-01-2018

    customer_id, first_name, last_name, relationship_id, card_type, max_limit, spent,cash_withdrawn,\
        payment_cleared, payment_date = element.split(',')

    spent = int(spent)
    payment_cleared = int(payment_cleared)
    max_limit = int(max_limit)

    key_name = customer_id + ', ' + first_name + ', ' + last_name   # 'CT28383, Miyako, Burns'
    defaulters_points = 0

    # payment cleared is less than 70% of spent -> give 1 point
    benchmark_default = payment_cleared < (spent * 0.7)
    if benchmark_default:                                           # defaulter_point = 1
        defaulters_points += 1

    # spend is = 100% of max limit and any amount of payment is pending
    max_credit_default = ( spent == max_limit ) and ( payment_cleared < spent)
    if max_credit_default:                                          # defaulter_point = 2
        defaulters_points += 1

    if benchmark_default and max_credit_default:                    # defaulter_point = 3
        defaulters_points += 1

    return key_name, defaulters_points                              # ('CT28383, Miyako, Burns', 2)


def calculate_personal_loan_defaulter(input):
    max_allowed_missed_month = 4
    max_allowed_consecutive_missing = 2

    name, month_list = input     # [CT88330,Humberto,Banks,Serviceman,LN_1559,Medical Loan,26-01-2018, 2000, 30-01-2018]

    month_list.sort()
    sorted_months = month_list
    total_payment = len(sorted_months)

    missed_payments = 12 - total_payment

    if missed_payments > max_allowed_missed_month:
        return name, missed_payments

    consecutive_missed_months = 0

    temp = sorted_months[1] - sorted_months[0]
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp

    temp = 12 - sorted_months[total_payment - 1]
    if temp > consecutive_missed_months:
        consecutive_missed_months = temp

    for i in range(1, len(sorted_months)):
        temp = sorted_months[i] - sorted_months[i-1] - 1
        if temp > consecutive_missed_months:
            consecutive_missed_months = temp

    if consecutive_missed_months > max_allowed_consecutive_missing:
        return name, consecutive_missed_months

    return name, 0


def return_tuple(element):
    this_tuple = element.split(',')
    return this_tuple[0], this_tuple[1:]


def full_pipeline():
    defaulters = beam.Pipeline()

    card_defaulters = (
            defaulters
            | 'Read credit card data' >> beam.io.ReadFromText('cards.txt', skip_header_lines=1)
            | 'Calculate defaulter points' >> beam.Map(calculate_points_cards)
            | 'Combine points for defaulters' >> beam.CombinePerKey(sum)
            | 'Filter card defaulters' >> beam.Filter(lambda element: element[1] > 0)
            | 'Format output' >> beam.Map(format_result)
            #| 'Write credit card data' >> beam.io.WriteToText('card_defaulters')
            | 'tuple' >> beam.Map(return_tuple)
    )

    medical_loan_defaulter = (
        defaulters
        | "Read from text file" >> beam.io.ReadFromText('loan.txt', skip_header_lines=1)
        | "split row" >> beam.Map(lambda row: row.split(","))
        | "filter medical loan" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Medical Loan')
        | "calculate late payment" >> beam.Map(calculate_late_payment)
        | "Make key value pairs" >> beam.Map(lambda element: (element[0]+", "+element[1]+" "+element[2], int(element[9])))
        | "Group medical loans based on month" >> beam.CombinePerKey(sum)
        | "Filter only medical loan defaulter" >> beam.Filter(lambda element: element[1] >= 3)
        | "format medical loan output" >> beam.Map(format_output)
    )

    personal_loan_defaulter = (
        defaulters
        | "Read" >> beam.io.ReadFromText('loan.txt', skip_header_lines=1)
        | "Split" >> beam.Map(lambda row: row.split(","))
        | "filter personal loan" >> beam.Filter(lambda element: (element[5]).rstrip().lstrip() == 'Personal Loan')
        | "calculate month" >> beam.Map(calculate_month)
        | "k_v pairs" >> beam.Map(lambda element: (element[0] + ", " + element[1] + " " + element[2], int(element[9])))
        | "Group personal loans based on month" >> beam.GroupByKey()
        | "check personal loan defaulter" >> beam.Map(calculate_personal_loan_defaulter)
        | "Filter only personal loan defaulter" >> beam.Filter(lambda element: element[1] > 0)
        | "format personal loan output" >> beam.Map(format_output)
    )

    final_loan_defaulters = (
        (personal_loan_defaulter, medical_loan_defaulter)
        | "combine all defaulters" >> beam.Flatten()
        #|
        | 'tuple for loan' >> beam.Map(return_tuple)
    )

    both_defaulters = (
        {"card_defaulter": card_defaulters, "loan_defaulter": final_loan_defaulters}
        | beam.CoGroupByKey()
        | "write result" >> beam.io.WriteToText('both')
    )

    defaulters.run()


full_pipeline()