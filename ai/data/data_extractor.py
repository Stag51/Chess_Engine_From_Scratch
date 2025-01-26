# Author : Saad Shabbir
# File to extract chess games data from the web
from ai.data.parser import Parser
from time import localtime
from os import listdir
from urllib.request import urlopen
from bz2 import BZ2File
from re import split
import traceback


class DataExtractor(Parser):
    def __init__(self, game, location, logger):
        super().__init__(game, location, logger)
        self.destination = 'ai/data/dataset/'
        self.raw_data = {
            'link_start': 'https://database.lichess.org/standard/lichess_db_standard_rated_',
            'link_end': '.pgn.zst',  # .pgn.bz2?
            'start_year': 2018,
            'cur_year': localtime()[0] + 1,
            'cur_month': localtime()[1],
            'total': (localtime()[0] - 2018) * 12 + localtime()[1] - 1
        }

    @property
    def memory(self):
        location = self.destination + 'memory.py'
        data = []
        with open(location, 'r') as fp:
            for line in fp:
                data.append(eval(line.strip()))
        return data

    @memory.setter
    def memory(self, data):
        location = self.destination + 'memory.py'
        with open(location, 'w') as fp:
            for moves in data:
                fp.write(str(moves) + '\n')

    def clean_memory(self):
        self.logger.info('Cleaning memory...')
        old_data = self.memory
        data = []
        for i, dp in enumerate(old_data[:25]):
            cur_dict = {}
            for board in dp:
                if (i < 8 and dp[board] >= 9) \
                        or (i < 16 and dp[board] >= 8) \
                        or (i < 21 and dp[board] >= 6) \
                        or (i < 25 and dp[board] >= 4):
                    cur_dict[board] = dp[board]
            data.append(cur_dict)
        self.memory = data

    def clean_data(self):
        self.logger.info('Cleaning data...')
        data = []
        with open(self.destination + 'data_0.txt') as fp:
            for line in fp:
                if len(line) >= 150:
                    data.append(line)
        with open(self.destination + 'data_0.txt', 'w') as fp:
            for line in data:
                fp.write(line)

    def datapoints(self, num_games):
        data = open(self.location + '/train_state.txt').readlines()
        datafile, line = int(data[0].strip()), int(data[1])
        skip_lines = line

        while num_games != 0:
            with open(self.destination + f'data_{datafile}.txt') as fp:
                for moves in fp:
                    if num_games == 0:
                        break
                    if skip_lines != 0:
                        skip_lines -= 1
                        continue
                    try:
                        games = self._generate_datapoint(moves)
                        num_games -= 1
                        line += 1
                        yield games
                    except ValueError:
                        self.logger.error(f'Fix this bug!!\n{traceback.format_exc()}')
                        self.logger.error(f'moves is:\n{moves}')
                        self.logger.error(f'line is: {line}')
                        continue
                    except Exception:
                        self.logger.error(f'Exception occurred!\n{traceback.format_exc()}')
                        continue
            if num_games != 0:
                datafile += 1
                line = 0
        with open(self.location + '/train_state.txt', 'w') as fp:
            fp.write(f'{datafile}\n{line}')
        return StopIteration

    def download_raw_data(self):
        self.logger.info('Begin processing dataset')
        for year in range(self.raw_data['start_year'], self.raw_data['cur_year']):
            for month in range(1, 13):
                if year == self.raw_data['cur_year'] - 1 and month == self.raw_data['cur_month']:
                    break
                link_ID = f"{year}-{month:02d}"
                link = f"{self.raw_data['link_start']}{link_ID}{self.raw_data['link_end']}"
                self.logger.info(f'Processing data: {year}:{month}...{((year - self.raw_data["start_year"]) * 12 + (month - 1)) * 100 // self.raw_data["total"]}% done')
                filename = f'{self.destination}data_{year}_{month}'
                lines = BZ2File(urlopen(link), 'r')
                memory = self.memory
                try:
                    self._process_data(filename, iter(lines), memory)
                except (StopIteration, EOFError):
                    self.logger.info('File Complete')
                except Exception as e:
                    self.logger.error(f'Exception occurred!\n{traceback.format_exc()}')
                    raise e
                self.memory = memory
        self.logger.info('Finish processing dataset')

    def _process_data(self, filename, dataset, memory):
        file_ID = 0
        while True:
            games_processed = 0
            with open(f'{filename}_{file_ID}.txt', 'w') as state:
                white_elo, black_elo = 0, 0
                while games_processed < 100000:
                    try:
                        line = next(dataset).decode('utf-8')
                    except StopIteration:
                        break
                    if 'WhiteElo' in line:
                        white_elo = split('"', line)[1]
                        if not white_elo.startswith('2'):
                            white_elo = 0
                            continue
                        white_elo = int(white_elo)
                    elif 'BlackElo' in line:
                        black_elo = split('"', line)[1]
                        if not black_elo.startswith('2'):
                            black_elo = 0
                            continue
                        black_elo = int(black_elo)
                    elif line.startswith('1') and min(white_elo, black_elo) >= 2500:
                        games_processed += 1
                        datapoint = self._raw_data_to_datapoint(line)
                        boards = self._generate_datapoint(datapoint)
                        for i, board in enumerate(boards[0]):
                            memory[i][repr(board)] = memory[i].get(repr(board), 0) + 1
                        white_elo, black_elo = 0, 0
                        state.write(datapoint)
            file_ID += 1
        raise EOFError