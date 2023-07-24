import pybaseball
import pandas as pd
import numpy as np
import os
from collections import defaultdict
import json


pybaseball.cache.enable()

latest_column_added = 'description_cat'

years = ['2023']

start_dates = {'2021': '2021-04-01',
               '2022': '2022-04-07',
               '2023': '2023-03-30'}
end_dates = {'2021': '2021-11-02',
             '2022': '2022-11-05',
             '2023': '2023-07-10'}

categorize_description = {'hit_into_play': 'P',
                          'foul': 'F',
                          'ball': 'B',
                          'foul_tip': 'S',
                          'swinging_strike': 'S',
                          'swinging_strike_blocked': 'S',
                          'called_strike': 'S',
                          'foul_bunt': 'S',
                          'blocked_ball': 'B',
                          'hit_by_pitch': 'HBP',
                          'missed_bunt': 'S',
                          'pitchout': 'X',
                          'bunt_foul_tip': 'S',}

categorize_description = defaultdict(lambda: 'X', categorize_description)

# Possible outs, counts, and base situations
p_outs = [0,1,2]
p_counts = ['00', '01', '02', '10', '11', '12', '20', '21', '22', '30', '31', '32']
p_bases = ['XXX', 'OXX', 'XOX', 'OOX', 'XXO', 'OXO', 'XOO', 'OOO']


def generate_count(x):
    """uses balls and strikes"""
    return str(x[0]) + str(x[1])

def generate_inning_code(x):
    """uses game_pk, inning, inning_topbot"""
    return str(x[0]) + str(x[1]) + str(x[2])

def situation_to_identifier(x):
    """uses outs, counts, bases"""
    first = x[2]
    second = x[3]
    third = x[4]
    output = str(x[0]) + x[1]
    for c in [first, second, third]:
        if c:
            output += 'O'
        else:
            output += 'X'
    return output


def pitch_logic(key, result):
    """Takes a situation identifier and determines the new situation based on whether the outcome is S, B, or F"""
    outs = int(key[0])
    balls = int(key[1])
    strikes = int(key[2])
    first = key[3] == 'O'
    second = key[4] == 'O'
    third = key[5] == 'O'
    extra = ''
    if result == 'B':
        balls += 1
        if balls >= 4:
            balls = 0
            strikes = 0
            if first:
                if second:
                    if third:
                        extra = '+'
                    else:
                        third = True
                else:
                    second = True
            else:
                first = True
    elif result == 'S':
        strikes += 1
        if strikes >= 3:
            balls = 0
            strikes = 0
            outs += 1
            if outs == 3:
                return 'INNING_OVER'
    elif result == 'F':
        if strikes == 2:
            return key
        else:
            strikes += 1
                        
        
    output = f'{outs}{balls}{strikes}'
    for runner in [first, second, third]:
        if runner:
            output += 'O'
        else:
            output += 'X'
    return output + extra
    

for year in years:
    print('beginning process for year ' + year)
    filename = year + '_mlb_statcast.csv'
    if os.path.isfile(filename):
        print('local data found.')
        df = pd.read_csv(filename)
        print('dataframe loaded')
    else:
        print('local data not found. downloading statcast data...')
        df = pybaseball.statcast(start_dt = start_dates[year], end_dt = end_dates[year]).reset_index(drop = True)
        df.to_csv(filename) 
        print('local data saved.')
    

    if latest_column_added not in df.columns:
        # df preprocessing. will cache when done.
        df['count'] = df[['balls', 'strikes']].apply(generate_count, axis = 1)

        print('generating inning codes. this may take a while...')
        df['inning_code'] = df[['game_pk', 'inning', 'inning_topbot']].apply(generate_inning_code, axis = 1)
        inning_codes = df['inning_code'].unique()
        print('inning codes generated.')

        runs_to_score = {}
        i = 0
        j = len(inning_codes)
        for inning_code in inning_codes:
            print(f'determining runs scored in each inning -- iteration {i}/{j}', end = '\r')
            dfinn = df.loc[df['inning_code'] == inning_code]
            champ = dfinn.sort_values(by = 'at_bat_number', ascending = False).iloc[0]['post_bat_score']
            runs_to_score[inning_code] = champ
            i += 1
        print('runs scored in each inning successfully determined.')
            
        df['post_inn_score'] = df['inning_code'].apply(lambda x: runs_to_score[x])
        df['runs_to_score'] = df['post_inn_score'] - df['bat_score']
        print('remaining runs to score per situation calculated.')

        for c in ['on_1b', 'on_2b', 'on_3b']:
            df[c] = df[c].fillna(0)
        df['rofirst'] = df['on_1b'].apply(lambda x: x > 0)
        df['rosecond'] = df['on_2b'].apply(lambda x: x > 0)
        df['rothird'] = df['on_3b'].apply(lambda x: x > 0)
        df['situation_identifier'] = df[['outs_when_up', 'count', 'rofirst', 'rosecond', 'rothird']].apply(situation_to_identifier, axis = 1)
        df['description_cat'] = df['description'].apply(lambda x: categorize_description[x])
        print('pitch descriptions categorized.')

        print('preprocessing complete.')
        df.to_csv(filename) 
        print('local data saved.')  
    else:
        print("column '" + latest_column_added + "' found, skipping preprocessing steps.")
        pass
    

    re288 = {}
    total_cat = {}
    for c in ['S', 'B', 'F']:
        total_cat[c] = len(df.loc[df['description_cat'] == c])
        
    for outs in p_outs:
        for count in p_counts:
            for bases in p_bases:
                key = str(outs) + count + bases
                re288[key] = {}
                re288[key]['cat_frequency'] = {}
                for c in ['S', 'B', 'F']:
                    re288[key][c] = pitch_logic(key, c)
                    view = df.loc[df['situation_identifier'] == key]
                    value = view['runs_to_score'].mean()
                    frequency = len(view) / len(df)
                    re288[key]['value'] = value
                    re288[key]['frequency'] = frequency
                    re288[key]['cat_frequency'][c] = len(view.loc[view['description_cat'] == c])/ total_cat[c]

    print('re288 calculated.')


    generic_values = {'S': 0, 'B': 0, 'F': 0} 
    specific_values = {'S': 0, 'B': 0, 'F': 0}

    re288['INNING_OVER'] = {'value': 0}
    for key in ['000OOO+', '100OOO+', '200OOO+']:
        re288[key] = {'value': re288[key[:-1]]['value'] + 1}
        
    specific_values['ball_to_strike'] = 0
    specific_values['strike_to_ball'] = 0

    for outs in p_outs:
        for count in p_counts:
            for bases in p_bases:
                key = str(outs) + count + bases
                re288[key]['value_change_if:'] = {}
                for c in ['S', 'B', 'F']:
                    re288[key]['value_change_if:'][c] = (re288[re288[key][c]]['value'] - re288[key]['value'])
                    generic_values[c] += re288[key]['value_change_if:'][c] * re288[key]['frequency']
                    specific_values[c] += re288[key]['value_change_if:'][c] * re288[key]['cat_frequency'][c]
                specific_values['ball_to_strike'] += (re288[re288[key]['B']]['value'] - re288[re288[key]['S']]['value']) * re288[key]['cat_frequency']['B']
                specific_values['strike_to_ball'] += (re288[re288[key]['S']]['value'] - re288[re288[key]['B']]['value']) * re288[key]['cat_frequency']['B']
    
    print('values for balls and strikes calculated.')
                    
    generic_values['ball_to_strike'] = generic_values['B'] - generic_values['S']
    generic_values['strike_to_ball'] = generic_values['S'] - generic_values['B']

    counts = ['02', '12', '01', '22', '11', '00', '10', '21', '32', '20', '31', '30']
    redf = pd.DataFrame(columns = counts)
    pitchvaluedf = {}
    for c in ['S', 'B', 'F']:
        pitchvaluedf[c] = pd.DataFrame(columns = counts)

    for outs in ['0', '1', '2']:
        for runners in ['XXX', 'OXX', 'XOX', 'OOX', 'XXO', 'OXO', 'XOO', 'OOO']:
            rows = {'re288': [], 'S': [], 'B': [], 'F': []}
            for count in counts:
                key = outs + count + runners
                for c in ['S', 'B', 'F']:
                    rows[c].append(re288[key]['value_change_if:'][c])
                rows['re288'].append(re288[key]['value'])
            redf.loc[outs + 'out' + runners] = rows['re288']
            for c in ['S', 'B', 'F']:
                pitchvaluedf[c].loc[outs + 'out' + runners] = rows[c]


    redf.round(2).to_csv(year+'re288.csv')
    pitchvaluedf['S'].round(3).to_csv(year + 'strike_values_288.csv')
    pitchvaluedf['B'].round(3).to_csv(year + 'ball_values_288.csv')
    pitchvaluedf['F'].round(3).to_csv(year + 'foul_values_288.csv')

    print(year+'re288.csv saved to disk.')


    with open(year + 're288.txt', 'w') as file:
        file.write(json.dumps(re288))
    with open(year + '_SBF_values_generic.txt', 'w') as file:
        file.write(json.dumps(generic_values))
    with open(year + '_SBF_values_specific.txt', 'w') as file:
        file.write(json.dumps(specific_values))
    
    print('ball and strike values saved to disk.')
                    



