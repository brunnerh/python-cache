import os
import os.path as path
import shutil
import unittest
from cache import Cache, format_as_timestamp
from pathlib import Path
from datetime import datetime, timedelta

test_folder = 'test-files'

db_file_name = path.join(test_folder, 'test.db')
table_name = 'cache'
folder_path = path.join(test_folder, 'cache-files')

def createTestFile():
	new_file_name = 'test-file.txt'
	new_file_path = path.join(test_folder, new_file_name)
	open(new_file_path, 'w').close()

	return [new_file_name, new_file_path]

class Test(unittest.TestCase):
	def createCache(self):
		self.cache = Cache(db_file_name, table_name, folder_path)
		return self.cache

	def setUp(self):
		if path.exists(test_folder):
			shutil.rmtree(test_folder)

		Path(test_folder).mkdir(parents=True, exist_ok=True)

	def tearDown(self):
		if self.cache is not None:
			self.cache.connection.close()

	def test_initializes_db(self):
		cache = self.createCache()
		c = cache.connection.cursor()
		c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (cache.table_name,))
		name = c.fetchone()[0]
		c.close()
		self.assertEqual(table_name, name)

	def test_initializes_folder(self):
		cache = self.createCache()
		exists = os.path.exists(folder_path)
		self.assertTrue(exists)

	def test_file_name_exists(self):
		cache = self.createCache()
		open(path.join(folder_path, 'test-file.txt'), 'w').close()

		self.assertTrue(cache.file_name_exists('test-file.txt'))
		self.assertFalse(cache.file_name_exists('not-a-test-file.txt'))

	def test_add_file(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1234', new_file_path, new_file_name, copy_file=False)

		c = cache.connection.cursor()
		rows = c.execute('select * from cache').fetchall()
		c.close()
		self.assertEqual(1, len(rows))
		self.assertEqual('1234', rows[0][0])
		self.assertEqual(new_file_name, rows[0][1])

		self.assertFalse(path.exists(new_file_path))
		self.assertTrue(path.exists(path.join(cache.folder_path, new_file_name)))

	def test_add_file_copy(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1234', new_file_path, new_file_name, copy_file=True)

		self.assertTrue(path.exists(new_file_path))
		self.assertTrue(path.exists(path.join(cache.folder_path, new_file_name)))

	def test_add_file_existing_names(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1', new_file_path, new_file_name, copy_file=True)
		cache.add_file('2', new_file_path, new_file_name, copy_file=True)
		cache.add_file('3', new_file_path, new_file_name, copy_file=True)

		c = cache.connection.cursor()
		rows = c.execute('select * from cache').fetchall()
		c.close()
		names = [x[1] for x in rows]
		self.assertListEqual(
			['test-file.txt', 'test-file (2).txt', 'test-file (3).txt'],
			names
		)
		for name in names:
			self.assertTrue(path.exists(path.join(folder_path, name)))
	
	def test_get_file_path(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1234', new_file_path, new_file_name, copy_file=False)
		file_path = cache.get_file_path('1234')

		self.assertEqual(file_path, path.join(folder_path, new_file_name))

	def test_delete_file(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		path1 = cache.add_file('1', new_file_path, new_file_name, copy_file=True)
		path2 = cache.add_file('2', new_file_path, new_file_name, copy_file=True)
		path3 = cache.add_file('3', new_file_path, new_file_name, copy_file=True)
		errors = cache.delete_file('2')

		self.assertEqual(0, len(errors))

		self.assertTrue(path.exists(path1))
		self.assertFalse(path.exists(path2))
		self.assertTrue(path.exists(path3))

		c = cache.connection.cursor()
		rows = c.execute('select * from cache').fetchall()
		self.assertEqual(2, len(rows))
		self.assertListEqual(['1', '3'], [row[0] for row in rows])
		c.close()

	def test_clear(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1', new_file_path, new_file_name, copy_file=True)
		cache.add_file('2', new_file_path, new_file_name, copy_file=True)
		cache.add_file('3', new_file_path, new_file_name, copy_file=True)
		# Delete one of the files to provoke error
		os.remove(path.join(folder_path, new_file_name))

		errors = cache.clear()

		self.assertEqual(1, len(errors))
		self.assertEqual('1', errors[0][0])

		c = cache.connection.cursor()
		rows = c.execute('select * from cache').fetchall()
		self.assertEqual(1, len(rows))
		self.assertEqual('1', rows[0][0])
		c.close()

	def test_delete_older_than(self):
		cache = self.createCache()
		[new_file_name, new_file_path] = createTestFile()

		cache.add_file('1', new_file_path, new_file_name, copy_file=True)
		cache.add_file('2', new_file_path, new_file_name, copy_file=True)
		cache.add_file('3', new_file_path, new_file_name, copy_file=True)
		# Manipulate times
		c = cache.connection.cursor()
		def update_minus_days(key, days):
			dt = (datetime.now() - timedelta(days=days)).astimezone()
			new_dt = format_as_timestamp(dt)
			c.execute("update cache set timestamp = ? where key = ?", (new_dt, key))

		update_minus_days('1', 31) # should delete
		update_minus_days('2', 29) # should not delete
		cache.connection.commit()

		cache.delete_older_than(timedelta(days=30))

		rows = c.execute('select * from cache').fetchall()
		self.assertEqual(2, len(rows))
		c.close()

if __name__ == '__main__':
	unittest.main()