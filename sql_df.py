from sqlalchemy import Table
from sqlalchemy.orm.decl_api import DeclarativeMeta

import pandas as pd


class SQLDataFrame:
    SQL_WRITE_CHUNK_SIZE = 100000
    FULL = 'ALL_COLUMNS'
    AVAILABLE_COLUMNS = 'AVAILABLE_COLUMNS'

    def __init__(self, model, con, read: bool | dict | None = None):

        # TODO прописать какое-нибудь описание для всего этого.
        # TODO и как итог это все люто медленное при повторной попытке перезаписать все.
        # TODO а еще было б классно обмазать тут все логированием.
        self.__read_params = {}
        self.df = pd.DataFrame()
        self.__con = con

        if isinstance(model, Table):
            self.__table = model
        elif isinstance(model, DeclarativeMeta):
            self.__table = model.__table__
        else:
            raise TypeError('"model" should be sqlalchemy.Table or sqlalchemy.orm.decl_api.DeclarativeMeta.')

        self.__columns = [column.name for column in self.__table.columns]
        self.__sql_table_name = self.__table.name

        if read:
            if isinstance(read, dict):
                self.__read_params = read
            self.read(**self.__read_params)

    def read(self, **kwargs):
        self.__read_params = kwargs
        if kwargs.get('only'):
            return self.read_only(kwargs['only'])
        return self.read_table(**kwargs)

    def read_only(self, col: str):
        """Примерно в 1,5 раза быстрее чем read_table для одной колонки"""
        self.df = pd.read_sql_query(f'SELECT {col} FROM {self.__sql_table_name}', con=self.__con)

    def read_table(self, **kwargs):
        self.df = pd.read_sql_table(self.__sql_table_name, con=self.__con, **kwargs)

    def refresh(self):
        self.read(**self.__read_params)

    def write_data_frame(self, df: pd.DataFrame, refresh: bool = False):
        if not df.empty:
            columns = [col for col in self.__columns if col in df.columns.to_list()]
            write_line_count = df[columns].to_sql(self.__sql_table_name, con=self.__con, if_exists='append',
                                                  index=False, chunksize=self.SQL_WRITE_CHUNK_SIZE)
            if refresh and write_line_count:
                self.refresh()
            return write_line_count

    def write(self, refresh: bool = False):
        return self.write_data_frame(self.df, refresh)

    def write_new(self, df: pd.DataFrame, check_by=None, refresh=True):
        check_by = check_by or 'id'

        if check_by == self.FULL:
            check_by = self.__columns

        if check_by == self.AVAILABLE_COLUMNS:
            check_by = self.df.columns.to_list()

        if isinstance(check_by, str):
            check_by = [check_by]

        if any(cb not in self.__columns for cb in check_by):
            raise KeyError(f'{check_by=}, not in sql table {self.__sql_table_name}')

        if any(cb not in df.columns.to_list() for cb in check_by):
            raise KeyError(f'{check_by=}, not in data frame.{df.columns.to_list()=}')

        if any(cb not in self.df.columns.to_list() for cb in check_by):
            raise KeyError(f'{check_by=}, not in actual data frame. Try SQLDataFrame.read(columns={check_by})')

        masc = pd.DataFrame([df[column].isin(self.df[column]) for column in check_by]).all()
        # TODO Скорее всего есть способ проще.
        return self.write_data_frame(df[~masc], refresh)
