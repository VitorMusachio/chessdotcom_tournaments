import json
import pandas as pd

from urllib import request
from bs4 import BeautifulSoup

def remove_prefix(column, prefix):
    """
    Helper function to remove a prefix from each element in a pandas DataFrame column.
    """
    return column.apply(lambda x: x.replace(prefix, ''))

def process_tournaments(tournament_list):
    """
    Process tournament data from a list of tournament IDs.
    """
    all_data = []

    for tournament in tournament_list:
        # get chess.com json
        url = "https://api.chess.com/pub/tournament/" + tournament
        data = request.urlopen(url).read()
        soup = BeautifulSoup(data, 'html.parser')
        site_json = json.loads(soup.text)

        # main data prep
        game_data = site_json['games']
        url = [d.get('url') for d in game_data if d.get('url')]
        time_control = [d.get('time_control') for d in game_data if d.get('time_control')]
        end_time = [d.get('end_time') for d in game_data if d.get('end_time')]
        rated = [d.get('rated') for d in game_data if d.get('rated')]
        fen = [d.get('fen') for d in game_data if d.get('fen')]
        time_class = [d.get('time_class') for d in game_data if d.get('time_class')]
        rules = [d.get('rules') for d in game_data if d.get('rules')]
        eco = [d.get('eco') for d in game_data if d.get('eco')]

        # create games dataframe
        df_games = pd.DataFrame({
            'url': url,
            'time_control': time_control,
            'end_time': end_time,
            'rated': rated,
            'fen': fen,
            'time_class': time_class,
            'rules': rules,
            'eco': eco
        })

        # white players data prep
        white = [d.get('white') for d in game_data if d.get('white')]
        df_white_players = pd.DataFrame(data=white)
        df_white_players.columns = ['white_rating', 'white_result', 'white_id', 'white_username']

        # black players data prep
        black = [d.get('black') for d in game_data if d.get('black')]
        df_black_players = pd.DataFrame(data=black)
        df_black_players.columns = ['black_rating', 'black_result', 'black_id', 'black_username']

        # merge white and black players dataframes
        df_players = df_white_players.merge(df_black_players, left_index=True, right_index=True)

        # create pgn dataframe
        pgn = [d.get('pgn') for d in game_data if d.get('pgn')]
        df_pgn = pd.DataFrame(data=pgn)
        df_pgn.columns = ['pgn']

        # split columns
        df_pgn = df_pgn['pgn'].str.split("\n", expand=True)
        df_pgn.columns = ['event', 'site', 'date', 'round', 'white', 'black', 'result', 'current_position',
                          'timezone', 'eco', 'eco_url', 'utc_date', 'utc_time', 'white_elo', 'black_elo',
                          'time_control', 'termination', 'start_time', 'end_date', 'end_time', 'link', 'col_1',
                          'moves', 'col_2']

        # drop empty cols
        cols_drop = ['col_1', 'col_2']
        df_pgn = df_pgn.drop(cols_drop, axis=1)

        # pgn data cleaning
        df_pgn = df_pgn.apply(lambda x: x.replace('"', '', regex=True))
        df_pgn = df_pgn.apply(lambda x: x.replace('\]', '', regex=True))
        df_pgn = df_pgn.apply(lambda x: x.replace('\[', '', regex=True))

        # pgn data prep
        df_pgn['event'] = remove_prefix(df_pgn['event'], 'Event ')
        df_pgn['site'] = remove_prefix(df_pgn['site'], 'Site ')
        # ... (repetir para outras colunas)

        # create raw dataframe
        df_raw = df_games[['url', 'rated', 'fen', 'time_class', 'rules']].merge(
            df_players[['white_result', 'white_id', 'black_result', 'black_id']], left_index=True, right_index=True)

        df_raw = df_raw.merge(df_pgn, left_index=True, right_index=True)

        # feature engineering
        df_raw['game_id'] = df_raw['url'].apply(lambda x: x.replace('https://www.chess.com/game/live/', ''))
        df_raw['white_points'] = df_raw['result'].apply(lambda x: 1 if x == '1-0' else 0 if x == '0-1' else 0.5)
        df_raw['black_points'] = df_raw['result'].apply(lambda x: 1 if x == '0-1' else 0 if x == '1-0' else 0.5)

        df_raw['tournament_id'] = tournament
        df_raw['tournament_id'] = df_raw['tournament_id'].apply(lambda x: x[x.find("/") - 7:x.find("/")])
        df_raw['scrapy_date'] = pd.to_datetime("now", format='%Y-%m-%d %H:%M:%S')

        order_cols = ['game_id', 'url', 'event', 'tournament_id', 'site', 'rated', 'time_class', 'rules', 'date', 'round',
                      'time_control', 'result', 'termination', 'white', 'white_elo', 'white_id', 'white_result',
                      'white_points', 'black', 'black_elo', 'black_id', 'black_result', 'black_points', 'timezone',
                      'utc_date', 'utc_time', 'start_time', 'end_date', 'end_time', 'eco', 'eco_url',
                      'current_position', 'moves', 'scrapy_date']

        df = df_raw[order_cols].sort_values(by=['game_id'])

        # Append dataframes to the list
        all_data.append(df)

    # Concatenate all dataframes into a single dataframe
    result_df = pd.concat(all_data, ignore_index=True)

    return result_df

tournament_list = ['vii-arena-loggichess-1171620/1',
                   'another-tournament-id/1',
                   'yet-another-tournament-id/1']

df = process_tournaments(tournament_list)

# Save the final dataframe
df.to_csv('tournaments.csv', index=False)
