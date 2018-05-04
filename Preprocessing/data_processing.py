from __future__ import division
import sys
import csv
import time
import timeit
import pytz
import datetime
import cPickle as pickle
import os.path
import  numpy as np

import warnings
warnings.filterwarnings("ignore")


# country code for timezone
country_code_dict = {'DZA': 'DZ', 'AGO': 'AO', 'EGY': 'EG', 'BGD': 'BD', 'NER': 'NE', 'LIE': 'LI', 'NAM': 'NA',
                         'BGR': 'BG', 'BOL': 'BO', 'GHA': 'GH', 'CCK': 'CC', 'PAK': 'PK', 'CPV': 'CV', 'JOR': 'JO',
                         'LBR': 'LR', 'LBY': 'LY', 'MYS': 'MY', 'IOT': 'IO', 'PRI': 'PR', 'MYT': 'YT', 'PRK': 'KP',
                         'PSE': 'PS', 'TZA': 'TZ', 'BWA': 'BW', 'KHM': 'KH', 'UMI': 'UM', 'TTO': 'TT', 'PRY': 'PY',
                         'HKG': 'HK', 'SAU': 'SA', 'LBN': 'LB', 'SVN': 'SI', 'BFA': 'BF', 'SVK': 'SK', 'MRT': 'MR',
                         'HRV': 'HR', 'CHL': 'CL', 'CHN': 'CN', 'KNA': 'KN', 'JAM': 'JM', 'SMR': 'SM', 'GIB': 'GI',
                         'DJI': 'DJ', 'GIN': 'GN', 'FIN': 'FI', 'URY': 'UY', 'VAT': 'VA', 'STP': 'ST', 'SYC': 'SC',
                         'NPL': 'NP', 'CXR': 'CX', 'LAO': 'LA', 'YEM': 'YE', 'BVT': 'BV', 'ZAF': 'ZA', 'KIR': 'KI',
                         'PHL': 'PH', 'SXM': 'SX', 'ROU': 'RO', 'VIR': 'VI', 'SYR': 'SY', 'MAC': 'MO', 'NIC': 'NI',
                         'MLT': 'MT', 'KAZ': 'KZ', 'TCA': 'TC', 'PYF': 'PF', 'NIU': 'NU', 'DMA': 'DM', 'GBR': 'GB',
                         'BEN': 'BJ', 'GUF': 'GF', 'BEL': 'BE', 'MSR': 'MS', 'TGO': 'TG', 'DEU': 'DE', 'GUM': 'GU',
                         'LKA': 'LK', 'SSD': 'SS', 'FLK': 'FK', 'PCN': 'PN', 'BES': 'BQ', 'GUY': 'GY', 'CRI': 'CR',
                         'COK': 'CK', 'MAR': 'MA', 'MNP': 'MP', 'LSO': 'LS', 'HUN': 'HU', 'TKM': 'TM', 'SUR': 'SR',
                         'NLD': 'NL', 'BMU': 'BM', 'HMD': 'HM', 'TCD': 'TD', 'GEO': 'GE', 'MNE': 'ME', 'MNG': 'MN',
                         'MHL': 'MH', 'MTQ': 'MQ', 'BLZ': 'BZ', 'NFK': 'NF', 'MMR': 'MM', 'AFG': 'AF', 'BDI': 'BI',
                         'VGB': 'VG', 'BLR': 'BY', 'BLM': 'BL', 'GRD': 'GD', 'TKL': 'TK', 'GRC': 'GR', 'GRL': 'GL',
                         'SHN': 'SH', 'AND': 'AD', 'MOZ': 'MZ', 'TJK': 'TJ', 'THA': 'TH', 'HTI': 'HT', 'MEX': 'MX',
                         'ZWE': 'ZW', 'LCA': 'LC', 'IND': 'IN', 'LVA': 'LV', 'BTN': 'BT', 'VCT': 'VC', 'VNM': 'VN',
                         'NOR': 'NO', 'CZE': 'CZ', 'ATF': 'TF', 'ATG': 'AG', 'FJI': 'FJ', 'HND': 'HN', 'MUS': 'MU',
                         'DOM': 'DO', 'LUX': 'LU', 'ISR': 'IL', 'FSM': 'FM', 'PER': 'PE', 'REU': 'RE', 'IDN': 'ID',
                         'VUT': 'VU', 'MKD': 'MK', 'COD': 'CD', 'COG': 'CG', 'ISL': 'IS', 'GLP': 'GP', 'ETH': 'ET',
                         'COM': 'KM', 'COL': 'CO', 'NGA': 'NG', 'TLS': 'TL', 'TWN': 'TW', 'PRT': 'PT', 'MDA': 'MD',
                         'GGY': 'GG', 'MDG': 'MG', 'ATA': 'AQ', 'ECU': 'EC', 'SEN': 'SN', 'ESH': 'EH', 'MDV': 'MV',
                         'ASM': 'AS', 'SPM': 'PM', 'CUW': 'CW', 'FRA': 'FR', 'LTU': 'LT', 'RWA': 'RW', 'ZMB': 'ZM',
                         'GMB': 'GM', 'WLF': 'WF', 'JEY': 'JE', 'FRO': 'FO', 'GTM': 'GT', 'DNK': 'DK', 'IMN': 'IM',
                         'MAF': 'MF', 'AUS': 'AU', 'AUT': 'AT', 'SJM': 'SJ', 'VEN': 'VE', 'PLW': 'PW', 'KEN': 'KE',
                         'TUR': 'TR', 'ALB': 'AL', 'OMN': 'OM', 'TUV': 'TV', 'ALA': 'AX', 'BRN': 'BN', 'TUN': 'TN',
                         'RUS': 'RU', 'BRB': 'BB', 'BRA': 'BR', 'CIV': 'CI', 'SRB': 'RS', 'GNQ': 'GQ', 'USA': 'US',
                         'QAT': 'QA', 'WSM': 'WS', 'AZE': 'AZ', 'GNB': 'GW', 'SWZ': 'SZ', 'TON': 'TO', 'CAN': 'CA',
                         'UKR': 'UA', 'KOR': 'KR', 'AIA': 'AI', 'CAF': 'CF', 'CHE': 'CH', 'CYP': 'CY', 'BIH': 'BA',
                         'SGP': 'SG', 'SGS': 'GS', 'SOM': 'SO', 'UZB': 'UZ', 'CMR': 'CM', 'POL': 'PL', 'KWT': 'KW',
                         'ERI': 'ER', 'GAB': 'GA', 'CYM': 'KY', 'ARE': 'AE', 'EST': 'EE', 'MWI': 'MW', 'ESP': 'ES',
                         'IRQ': 'IQ', 'SLV': 'SV', 'MLI': 'ML', 'IRL': 'IE', 'IRN': 'IR', 'ABW': 'AW', 'SLE': 'SL',
                         'PAN': 'PA', 'SDN': 'SD', 'SLB': 'SB', 'NZL': 'NZ', 'MCO': 'MC', 'ITA': 'IT', 'JPN': 'JP',
                         'KGZ': 'KG', 'UGA': 'UG', 'NCL': 'NC', 'PNG': 'PG', 'ARG': 'AR', 'SWE': 'SE', 'BHS': 'BS',
                         'BHR': 'BH', 'ARM': 'AM', 'NRU': 'NR', 'CUB': 'CU'
                         }

# event type code
event_dict = {0: 'edx.course.enrollment.activated', 1: 'edx.course.enrollment.deactivated', 2: 'problem_check',
              3: 'problem_reset', 4: 'reset_problem', 5: 'problem_save', 6: 'save_problem_success', 7: 'save_problem_fail',
              8: 'problem_check_fail', 9: 'edx.forum.searched', 10: 'edx.forum.thread.created', 11: 'edx.forum.comment.created',
              12: 'edx.forum.response.created', 13: 'show_transcript', 14: 'hide_transcript', 15: 'load_video', 16: 'page_close',
              17: 'pause_video', 18: 'play_video', 19: 'seek_video', 20: 'stop_video', 21: 'speed_change_video',
              22: 'edx.forum.searched', 23: 'seq_next', 24: 'seq_prev', 25: 'seq_goto', 26: 'problem_get', 27: 'info',
              28: 'progress', 29: 'forum_view', 30: 'wiki', 31: 'about', 32: 'forum_thread_view', 33: 'login',
              34: 'edx.forum.response.voted', 35: 'edx.forum.thread.voted', 36: 'thread_follow', 37: 'forum_comment_updated',
              38: 'comment_flagAbuse_tag', 39: 'thread_flagAbuse_tag', 40: 'forum_thread_updated'}

def csvAdd(filename,ss):
    with open(filename,'a+') as f:
        f_csv = csv.writer(f)
        f_csv.writerow(ss)
    f.close()

def csvRead(filename):
    ff = csv.reader(open(filename,'rb'))
    return ff


def saveDictFile(dict, fileName):
    f = file(fileName,'wb')
    pickle.dump(dict,f)
    f.close()

def loadDictFile(fileName):
    if (os.path.isfile(fileName)) == True:
        modelDict = pickle.load(open(fileName, 'r'))
        return modelDict
    else:
        print 'load model function, file is not exist!'


def timezone_process(date_str, nation):
    ts = time.mktime(time.strptime(date_str, '%Y-%m-%d %H:%M:%S'))

    if nation in country_code_dict.keys():
        offset = datetime.datetime.now(pytz.timezone(pytz.country_timezones[country_code_dict[nation]][0])).strftime('%z')
        fix_ts = ts + int(offset) * 36
        fix_date = datetime.datetime.fromtimestamp(fix_ts).strftime('%Y-%m-%d %H:%M:%S')

    else:
        fix_date = None
        fix_ts = None

    return fix_date, fix_ts


def sequence_extract_from_raw(event_file, course):
    if course == 'cs101':
        ts = time.mktime(time.strptime('2014-07-14 00:00:00', '%Y-%m-%d %H:%M:%S'))
        te = time.mktime(time.strptime('2014-08-28 23:59:59', '%Y-%m-%d %H:%M:%S'))

    elif course == 'stat':
        ts = time.mktime(time.strptime('2015-01-19 00:00:00', '%Y-%m-%d %H:%M:%S'))
        te = time.mktime(time.strptime('2015-04-03 23:59:59', '%Y-%m-%d %H:%M:%S'))

    event_log = csvRead(event_file)
    seq_dict = {}

    for line in event_log:  # split event type and realted content into the sequence

        enroll_id = line[0]
        # print("line", line)
        # debugIndex += 1

        # if debugIndex > 50:
        #     break

        if (enroll_id != "'anon_screen_name'"):
            time_date = line[3]
            nation = line[2]

            if nation not in country_code_dict:
                continue
            else:

                time_fix, timestamp_fix = timezone_process(time_date, nation)
                # print(timestamp_fix, te, ts)
                if (timestamp_fix <= te) and (timestamp_fix >= ts):
                    if enroll_id not in seq_dict:
                        seq_dict[enroll_id] = {
                            'seq': {},
                            'nation': nation
                        }

                    ###actionType
                    action = line[1]
                    ###a
                    item = line[6]

                    success = line[7]

                    rr = [action, item, success]

                    # print("rr", rr)

                    if timestamp_fix not in seq_dict[enroll_id]['seq']:
                        seq_dict[enroll_id]['seq'][timestamp_fix] = [rr]
                    else:
                        seq_dict[enroll_id]['seq'][timestamp_fix].append(rr)

    saveDictFile(seq_dict, str(course) + '_sequence_extracted_official_range.txt')




######################################################################################
# main function
######################################################################################

if __name__ == '__main__':
    try:
        startT = timeit.default_timer()

        path_pre = '../../DATA/'   # path of the folder

        course_list = ['cs101', 'stat']
        course = course_list[1]          # select the course you need to process

        if course == 'cs101':
            Grade = path_pre + 'Engineering_CS101_Summer2014_ActivityGrade.csv'
            Event = path_pre + 'Engineering_CS101_Summer2014_EventXtract.csv'
            Video = path_pre + 'Engineering_CS101_Summer2014_VideoInteraction.csv'
            Demographic = path_pre + 'Engineering_CS101_Summer2014_demographics.csv'
            Survey = path_pre + 'Engineering_CS101_Summer2014_survey.csv'
            SurveyResponse = path_pre + 'Engineering_CS101_Summer2014_survey_responses.csv'
            SurveyResponseMeta = path_pre + 'Engineering_CS101_Summer2014_survey_response_metadata.csv'

        elif course == 'stat':
            Grade = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_ActivityGrade.csv'
            Event = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_EventXtract.csv'
            Video = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_VideoInteraction.csv'
            Demographic = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_demographics.csv'
            Survey = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_survey.csv'
            SurveyResponse = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_survey_responses.csv'
            SurveyResponseMeta = path_pre + 'HumanitiesandScience_StatLearning_Winter2015_survey_response_metadata.csv'

        sequence_extract_from_raw(Event, course)

        print 'time used    ', (timeit.default_timer() - startT)

    except Exception, e:
        print e