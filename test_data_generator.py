#!/usr/bin/env python

from string import Template
import uuid
import random
from datetime import datetime
import argparse
import logging
import sys

'''
Generate test data
'''
templates={
    "contract" :
        '{"_id": {"contractId": "$uuid"},"assessmentPeriods": ' \
           '[{"assessmentPeriodId": "$uuid","contractId": "$uuid","startDate": $date_int,"endDate": $date_int' \
           ',"paymentDate": $date_int,"processDate": null,' \
           '"createdDateTime": {"d_date": "$timestamp"}}],' \
           '"people": ["$uuid"],"declaredDate": $date_int,"startDate": $date_int,' \
           '"entitlementDate": $date_int,"closedDate": $date_int,"annualVerificationEligibilityDate": null,' \
           '"annualVerificationCompletionDate": null,"paymentDayOfMonth": $day,"flags": [],' \
           '"claimClosureReason": "FailedToBookInitialInterview","_version": 4,' \
           '"createdDateTime": {"d_date": "$timestamp"},"coupleContract": $boolean,' \
           '"claimantsExemptFromWaitingDays": [],"contractTypes": null,"claimSuspension": null,' \
           '"_entityVersion": 2,"_lastModifiedDateTime": {"d_date": "$timestamp"},"stillSingle": $boolean,' \
           '"contractType": "SUBSEQUENT"}'

 , "managementInformation" :
        '{"_id":{"managementInformationId":"$uuid"} ,"type":"PID_MATCHING"' \
                             ',"miSubEventValue":{ "MATCH_ID":"_d69cc43ea7467d5619ec950a4e777cd7","ACCOUNT_ID":"$uuid"} ' \
                             ',"createdDateTime":{"d_date":"$timestamp"},"_version":0,"miSubEvent":"IDA_005_PID_MATCH" ' \
                             ',"_entityVersion":0 ,"_lastModifiedDateTime":{"d_date":"$timestamp"}}'
 }

def t_date_int():

    return ("{:%Y%m%d}").format( datetime(random.randint(2014, 2017), random.randint(1, 12), random.randint(1, 28)))

def t_timestamp():
    return ("{:%Y-%m-%dT%H:%M:%S.000Z}").format( datetime(random.randint(2014, 2017)
                                                       , random.randint(1, 12)
                                                       , random.randint(1, 28)
                                                       , random.randint(0, 23)
                                                       , random.randint(0, 59)
                                                       , random.randint(0, 59)
                                                       ))

def t_day():
    return random.randint(1,31)

def t_boolean():
    return random.choice(["true", "false"])

def t_uuid():
    return str(uuid.uuid4())

def generate(template, lines):
    s = Template(templates[template])
    for i in range(0,lines):
        # initiate from from current time or from an operating system specific randomness source if available
        random.seed()

        # for each document generate separate set of values
        uuid_str = t_uuid()
        date_int = t_date_int()
        timestamp = t_timestamp()
        boolean = t_boolean()
        day = t_day()

        mapping = {'uuid': uuid_str, "date_int" : date_int, "timestamp" : timestamp, "boolean" : boolean, "day" : day}
        print s.safe_substitute(mapping)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--template", action = "store", required = False, help="json template", default="contract")
    parser.add_argument("-l", "--lines", action = "store", required = False, help="number of json lines", default="1000", type=int)
    args = parser.parse_args()

    generate(args.template, args.lines)


if __name__=="__main__":
    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout,level=logging.DEBUG, format='%(asctime)s %(levelname)s %(module)s %(message)s')
    main()

