using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLite.Net.Exceptions;
using SQLitePCL;

namespace SQLite.Net
{
    public class SQLiteQuery : SQLiteProgram
    {
	    private readonly Dictionary<string, int> _columns;
		
	    public int ColumnCount => raw.sqlite3_column_count(_stmt);
	    public IEnumerable<string> Columns => Enumerable.Range(0, ColumnCount).Select(GetColumnName);

	    internal SQLiteQuery(sqlite3 db, string sql) : base(db, sql)
	    {
		    _columns = Enumerable.Range(0, ColumnCount).ToDictionary(GetColumnName);
	    }

		public bool Step()
		{
			switch (raw.sqlite3_step(_stmt))
			{
				case raw.SQLITE_ROW:
					return true;
				case raw.SQLITE_DONE:
					return false;
				default:
					throw new SQLiteException(raw.sqlite3_errmsg(Db));
			}
		}

		public object this[int columnIndex] => GetValue(columnIndex);
		public object this[string column] => this[GetColumnIndex(column)];

	    public bool HasColumn(string name)
	    {
		    return _columns.ContainsKey(name);
	    }

	    public int GetColumnIndex(string columnName)
	    {
		    return _columns[columnName];
	    }

	    public string GetColumnName(int index)
		{
			CheckColumnIndex(index);

			return raw.sqlite3_column_name(_stmt, index);
	    }

	    public int GetColumnType(int index)
		{
			CheckColumnIndex(index);

			return raw.sqlite3_column_type(_stmt, index);
	    }

		public object GetValue(int columnIndex)
		{
			switch (GetColumnType(columnIndex))
			{
				case raw.SQLITE_INTEGER:
					return GetLong(columnIndex);
				case raw.SQLITE_FLOAT:
					return GetDouble(columnIndex);
				case raw.SQLITE_TEXT:
					return GetText(columnIndex);
				case raw.SQLITE_BLOB:
					return GetBlob(columnIndex);
				case raw.SQLITE_NULL:
					return null;
				default:
					return null;
			}
		}

	    public int GetInt(int columnIndex)
		{
			CheckColumnIndex(columnIndex);

			return raw.sqlite3_column_int(_stmt, columnIndex);
	    }

	    public double GetDouble(int columnIndex)
		{
			CheckColumnIndex(columnIndex);

			return raw.sqlite3_column_double(_stmt, columnIndex);
	    }

	    public string GetText(int columnIndex)
		{
			CheckColumnIndex(columnIndex);

			return raw.sqlite3_column_text(_stmt, columnIndex);
	    }

	    public byte[] GetBlob(int columnIndex)
		{
			CheckColumnIndex(columnIndex);

			byte[] result = raw.sqlite3_column_blob(_stmt, columnIndex);
		    if (result == null && !IsNull(columnIndex))
		    {
			    return Array.Empty<byte>();
		    }
		    return result;
	    }

	    public long GetLong(int columnIndex)
		{
			CheckColumnIndex(columnIndex);
			return raw.sqlite3_column_int64(_stmt, columnIndex);
	    }

	    public bool IsNull(int columnIndex)
		{
			CheckColumnIndex(columnIndex);
			return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_NULL;
		}

	    public bool IsString(int columnIndex)
		{
			CheckColumnIndex(columnIndex);
			return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_TEXT;
		}

	    public bool IsInt(int columnIndex)
		{
			CheckColumnIndex(columnIndex);
			return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_INTEGER;
		}

	    public bool IsFloat(int columnIndex)
		{
			CheckColumnIndex(columnIndex);
			return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_FLOAT;
		}

	    public bool IsBlob(int columnIndex)
	    {
			CheckColumnIndex(columnIndex);
		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_BLOB;
	    }

		private void CheckColumnIndex(int columnIndex)
		{
			if (columnIndex < 0 || columnIndex >= ColumnCount)
			{
				throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
			}
		}
	}
}
