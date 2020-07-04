__version__ = '0.1.0'

import os
import os.path as path
import sys
import shutil
import pathlib
import sqlite3
import threading
from pathlib import Path
from datetime import datetime, timedelta

timestamp_format = "%Y-%m-%dT%H:%M:%S%z"

def format_as_timestamp(dt: datetime):
	"""
	Formats a datetime as an ISO 8601 timestamp used in the database.

	:param dt: The datetime object to format.
	"""
	text = datetime.strftime(dt, timestamp_format)
	# Colon in timezone offset is required for SQLite date/time functions
	return "{0}:{1}".format(text[:-2], text[-2:])

class Cache:
	"""
	A class for caching files, backed by an SQLite database.
	"""

	def __init__(self, db_file_name: str, table_name: str, folder_path: str):
		"""
		Creates a new instance. DB, table and folder are created if they do not exist.

		:param db_file_name: Name of the SQLite database file.
		:param table_name: Name of the cache table in the database.
		:param folder_path: The folder where the cached files should be stored.
		"""
		self.db_file_name = db_file_name
		self.table_name = table_name
		self.folder_path = folder_path

		self._lock = threading.Lock()

		self.connection = sqlite3.connect(db_file_name)

		self._init_db()
		self._init_folder()

	def add_file(self, key: str, file_path: str, target_file_name: str, copy_file=True):
		"""
		Adds a file to the cache. If copy_file is false the file is moved.

		:param key: The key for the file in the cache.
		:param file_path: The path of the file to add.
		:param target_file_name: The name of the file in the cache directory.
		                         May be changed if a file with the same name exists.
		:param copy_file: Whether the file should be copied instead of moved.
		"""
		with self._lock:
			c = self.connection.cursor()
			c.execute(f'select key from {self.table_name} where key = ?', (key,))
			existing_key = c.fetchone()
			if existing_key is not None:
				raise Exception(f'Key already exists: {key}')

			target_file_name = self._get_unique_file_name(target_file_name)

			target_file_path = path.join(self.folder_path, target_file_name)
			if copy_file:
				shutil.copyfile(file_path, target_file_path)
			else:
				shutil.move(file_path, target_file_path)

			timestamp = format_as_timestamp(datetime.now().astimezone())

			c.execute(
				f'insert into {self.table_name} values (?, ?, ?)',
				(key, target_file_name, timestamp)
			)
			self.connection.commit()
			c.close()

			return self.get_file_path(key)

	def get_file_path(self, key: str):
		"""
		Gets the path of the file with the given key.
		None if key does not exist in the cache.
		"""
		c = self.connection.cursor()
		c.execute(f'select file_name from {self.table_name} where key = ?', (key,))
		row = c.fetchone()
		c.close()
		if row is None:
			return None

		return path.join(self.folder_path, row[0])

	def delete_file(self, key: str):
		"""
		Deletes a file from the cache.
		Returns tuples of (key, exception info) for files that could not be deleted.

		:param key: The key of the file in the database.
		"""
		return self._delete_entries(
			f'''
				select key, file_name from {self.table_name}
				where key = ?
			''',
			(key, )
		)

	def clear(self):
		"""
		Clears the cache, deleting all files.
		Returns tuples of (key, exception info) for files that could not be deleted.
		"""
		return self._delete_entries(f'select key, file_name from {self.table_name}', ())

	def delete_older_than(self, delta: timedelta):
		"""
		Removes entries older than the specified delta from the cache,
		including the files.
		Returns tuples of (key, exception info) for files that could not be deleted.

		:param delta: The time delta used to filter the entries by.
		"""
		now = (datetime.now() - delta).astimezone()
		return self._delete_entries(
			f'''
				select key, file_name from {self.table_name}
				where datetime(timestamp) < datetime(?)
			''',
			(format_as_timestamp(now),)
		)

	def _delete_entries(self, query, arguments):
		with self._lock:
			c = self.connection.cursor()
			entries = c.execute(query, arguments).fetchall()

			errors = []
			deleted = []
			for row in entries:
				try:
					os.remove(path.join(self.folder_path, row[1]))
					deleted.append(row[0])
				except:
					errors.append((row[0], sys.exc_info()))

			for key in deleted:
				c.execute(f'delete from {self.table_name} where key = ?', (key,))
			
			self.connection.commit()
			c.close()

			return errors

	def file_name_exists(self, file_name):
		"""
		Determines if there already is a file in the cache with the given name.

		:param file_name: The file name to check.
		"""
		return path.exists(path.join(self.folder_path, file_name))

	def _get_unique_file_name(self, target_file_name):
		if self.file_name_exists(target_file_name):
			counter = 2
			[base, ext] = path.splitext(target_file_name)
			getNewName = lambda: f'{base} ({counter}){ext}'
			while self.file_name_exists(getNewName()):
				counter = counter + 1

			target_file_name = getNewName()

		return target_file_name

	def _init_db(self):
		c = self.connection.cursor()

		c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (self.table_name,))
		name = c.fetchone()
		if name is None:
			c.execute(f'''
				create table {self.table_name}
				(
					key text primary key,
					file_name text,
					timestamp text
				)
			''')

		self.connection.commit()
		c.close()

	def _init_folder(self):
		Path(self.folder_path).mkdir(parents=True, exist_ok=True)