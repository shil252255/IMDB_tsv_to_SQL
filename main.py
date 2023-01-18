from pathlib import Path
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

from sql_df import SQLDataFrame
import pandas as pd
import numpy as np

from models import engine, BasicsTitle, Base, Genre, genres_association_table, Rating, TitlesAkas, Episode
from tests import print_time

DATA_FOLDER = Path("./files_gz")
TABLE_FILES = {file.name.split('.')[1]: file for file in DATA_FOLDER.glob('*.tsv.gz')}
READ_CHUNK_SIZE = 1000000

DTYPES = {'titleType': str,
          'primaryTitle': str,
          'originalTitle': str,
          'isAdult': str,
          'startYear': str,
          'endYear': str,
          'runtimeMinutes': str,
          }

TSV_READ_PARAMS = {
    'header': 0,
    'delimiter': '\t',
    'na_values': '\\N',
    'index_col': False,
    'chunksize': READ_CHUNK_SIZE,
    'quoting': 3,
    'encoding': 'utf-8',
    'keep_default_na': False,

}


def tconst_conv(tconst: str):
    return int(tconst[2:])


def list_conv(string: str):
    return string.split(',') if string != '\\N' else np.nan


title_read_params = TSV_READ_PARAMS.copy()
title_read_params.update({
    'dtype': DTYPES,  # вот тут дилема об использовании дататайпов ибо либо медленно, либо без проверки
    'names': [
        'id',
        'titleType',
        'primaryTitle',
        'originalTitle',
        'isAdult',
        'startYear',
        'endYear',
        'runtimeMinutes',
        'genres'
    ],
    'converters': {
        'id': tconst_conv,
        'genres': list_conv,
    }
})

ratings_read_params = TSV_READ_PARAMS.copy()
ratings_read_params.update({
    'names': [
        'title_id',
        'averageRating',
        'numVotes',
    ],
    'converters': {
        'title_id': tconst_conv,
    }
})

title_akas_read_params = TSV_READ_PARAMS.copy()
title_akas_read_params.update({
    'names': [
        'title_id',
        'ordering',
        'title',
        'region',
        'language',
        'types',
        'attributes',
        'isOriginalTitle'
    ],
    'converters': {
        'title_id': tconst_conv,
        # 'types': list_conv,
        # 'attributes': list_conv,
    }
})

title_episode_read_params = TSV_READ_PARAMS.copy()
title_episode_read_params.update({
    'names': [
        'self_id',
        'parent_id',
        'seasonNumber',
        'episodeNumber',
    ],
    'converters': {
        'self_id': tconst_conv,
        'parent_id': tconst_conv,
    }
})


def processing_basics_titles_file(file):
    genres = SQLDataFrame(Genre, engine, read=True)
    titles = SQLDataFrame(BasicsTitle, engine, read={'only': True, 'column': 'id'})
    genres_association = SQLDataFrame(genres_association_table, engine, read=True)

    for data in pd.read_csv(file, **title_read_params):
        titles.write_new(data, refresh=False)

        genres_assoc_from_csv = data[['id', 'genres']].dropna().explode('genres', ignore_index=True)

        genres.write_new(pd.DataFrame(genres_assoc_from_csv['genres'].unique(), columns=['name']), check_by='name')

        genres_assoc_from_csv['genres'].replace(genres.df.set_index('name')['id'], inplace=True)
        genres_assoc_from_csv.rename(columns={'id': 'title_id', 'genres': 'genre_id'}, inplace=True)

        genres_association.write_new(genres_assoc_from_csv, check_by=SQLDataFrame.FULL, refresh=False)


def processing_title_ratings_file(file):
    file_processing(file, SQLDataFrame(Rating, engine, read={'columns': ['title_id']}))


def processing_title_akas_file(file):
    file_processing(file, SQLDataFrame(TitlesAkas, engine, read=True))


def processing_title_episode_file(file):
    file_processing(file, SQLDataFrame(Episode, engine, read={'columns': ['self_id', 'parent_id']}))


def file_processing(file, sqlf_df):
    for data in pd.read_csv(file, **title_episode_read_params):
        sqlf_df.write_new(data, check_by=SQLDataFrame.AVAILABLE_COLUMNS, refresh=False)


if __name__ == '__main__':
    Base.metadata.create_all(engine)



