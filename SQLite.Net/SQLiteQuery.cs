using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SQLitePCL;

namespace SQLite.Net
{
    public class SQLiteQuery : SQLiteProgram
    {
	    private readonly Dictionary<string, int> _columns;

	    public int Position { get; private set; }
	    public int ColumnCount => raw.sqlite3_column_count(_stmt);

	    internal SQLiteQuery(sqlite3 db, string sql) : base(db, sql)
	    {
		    Position = 0;
		    _columns = Enumerable.Range(0, ColumnCount).ToDictionary(GetColumnName);
	    }

		public new bool Step()
		{
			if (!base.Step()) return false;
			Position++;
			return true;
		}

	    public object this[int columnIndex]
	    {
		    get
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
		}
		public object this[string column] => this[GetColumnIndex(column)];

	    public int GetColumnIndex(string columnName)
	    {
		    return _columns[columnName];
	    }

	    public string GetColumnName(int index)
		{
			return raw.sqlite3_column_name(_stmt, index);
	    }

	    public int GetColumnType(int index)
	    {
		    return raw.sqlite3_column_type(_stmt, index);
	    }

	    public int GetInt(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_int(_stmt, columnIndex);
	    }

	    public double GetDouble(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_double(_stmt, columnIndex);
	    }

	    public string GetText(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_text(_stmt, columnIndex);
	    }

	    public byte[] GetBlob(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

			byte[] result = raw.sqlite3_column_blob(_stmt, columnIndex);
		    if (result == null && !IsNull(columnIndex))
		    {
			    return Array.Empty<byte>();
		    }
		    return result;
	    }

	    public long GetLong(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_int64(_stmt, columnIndex);
	    }

	    public bool IsNull(int columnIndex)
	    {
			if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_NULL;
		}

	    public bool IsString(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_TEXT;
		}

	    public bool IsInt(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_INTEGER;
		}

	    public bool IsFloat(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_FLOAT;
		}

	    public bool IsBlob(int columnIndex)
	    {
		    if (columnIndex < 0 || columnIndex >= ColumnCount)
		    {
			    throw new IndexOutOfRangeException("Column Index " + columnIndex + " and Column Count " + ColumnCount);
		    }

		    return raw.sqlite3_column_type(_stmt, columnIndex) == raw.SQLITE_BLOB;
	    }
	}
}
