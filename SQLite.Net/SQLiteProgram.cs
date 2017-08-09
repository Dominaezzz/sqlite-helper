using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;
using SQLite.Net.Exceptions;
using SQLitePCL;

namespace SQLite.Net
{
    public class SQLiteProgram : IDisposable
    {
	    protected readonly sqlite3_stmt _stmt;

	    internal sqlite3 Db => raw.sqlite3_db_handle(_stmt);
	    public string Text => raw.sqlite3_sql(_stmt);
	    public int ParameterCount => raw.sqlite3_bind_parameter_count(_stmt);

	    internal SQLiteProgram(sqlite3 db, string sql)
	    {
			if (raw.sqlite3_prepare_v2(db, sql, out _stmt) != raw.SQLITE_OK)
			{
				throw new SQLiteException(raw.sqlite3_errmsg(db));
			}
		}

	    public int GetParameterIndex(string name)
	    {
		    return raw.sqlite3_bind_parameter_index(_stmt, name);
		}

	    public string GetParameterName(int index)
	    {
		    return raw.sqlite3_bind_parameter_name(_stmt, index);
	    }

		public void BindNull(int index)
	    {
		    switch (raw.sqlite3_bind_null(_stmt, index))
		    {
				case raw.SQLITE_OK:
					break;
				case raw.SQLITE_RANGE:
					throw new ArgumentOutOfRangeException(nameof(index), index, "");
				default:
					throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
	    }

	    public void Bind(int index, int value)
	    {
		    switch (raw.sqlite3_bind_int(_stmt, index, value))
		    {
			    case raw.SQLITE_OK:
				    break;
			    case raw.SQLITE_RANGE:
				    throw new ArgumentOutOfRangeException(nameof(index), index, "");
			    default:
				    throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
		}

	    public void Bind(int index, long value)
	    {
		    switch (raw.sqlite3_bind_int64(_stmt, index, value))
		    {
			    case raw.SQLITE_OK:
				    break;
			    case raw.SQLITE_RANGE:
				    throw new ArgumentOutOfRangeException(nameof(index), index, "");
			    default:
				    throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
		}

	    public void Bind(int index, byte[] value)
	    {
		    switch (raw.sqlite3_bind_blob(_stmt, index, value))
		    {
			    case raw.SQLITE_OK:
				    break;
			    case raw.SQLITE_RANGE:
				    throw new ArgumentOutOfRangeException(nameof(index), index, "");
			    case raw.SQLITE_TOOBIG:
			    default:
				    throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
		}

	    public void Bind(int index, double value)
	    {
		    switch (raw.sqlite3_bind_double(_stmt, index, value))
		    {
			    case raw.SQLITE_OK:
				    break;
			    case raw.SQLITE_RANGE:
				    throw new ArgumentOutOfRangeException(nameof(index), index, "");
			    default:
				    throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
		}

	    public void Bind(int index, string value)
	    {
		    switch (raw.sqlite3_bind_text(_stmt, index, value))
		    {
			    case raw.SQLITE_OK:
				    break;
			    case raw.SQLITE_RANGE:
				    throw new ArgumentOutOfRangeException(nameof(index), index, "");
			    case raw.SQLITE_TOOBIG:
			    default:
				    throw new SQLiteException(raw.sqlite3_errmsg(Db));
		    }
		}

	    public void Bind(int index, object value)
	    {
		    switch (Convert.GetTypeCode(value))
		    {
			    case TypeCode.Boolean:
					Bind(index, (bool)value ? 1 : 0);
				    break;
			    case TypeCode.DateTime:
				    Bind(index, ((DateTime) value).ToString(Orm.DateTimeFormat));
				    break;
			    case TypeCode.Decimal: case TypeCode.Double: case TypeCode.Single:
				    Bind(index, Convert.ToDouble(value));
					break;
			    case TypeCode.Empty:
					BindNull(index);
				    break;
			    case TypeCode.Byte: case TypeCode.SByte:
				case TypeCode.Int16: case TypeCode.UInt16:
				case TypeCode.Int32:
					Bind(index, Convert.ToInt32(value));
				    break;
			    case TypeCode.UInt32:
			    case TypeCode.Int64: case TypeCode.UInt64:
					Bind(index, Convert.ToInt64(value));
				    break;
			    case TypeCode.Char:
			    case TypeCode.String:
				    Bind(index, Convert.ToString(value));
					break;
			    case TypeCode.Object:
				    if (value.GetType().GetTypeInfo().IsEnum)
				    {
					    if (value.GetType().GetTypeInfo().IsDefined(typeof(StoreAsTextAttribute)))
					    {
							Bind(index, value.ToString());
					    }
					    else
					    {
							Bind(index, Array.IndexOf(Enum.GetValues(value.GetType()), value));
					    }
				    }
					switch (value)
				    {
						case byte[] blob:
							Bind(index, blob);
							break;
						case TimeSpan timeSpan:
							Bind(index, timeSpan.Ticks);
							break;
						case DateTimeOffset dateTimeOffset:
							Bind(index, dateTimeOffset.ToString(Orm.DateTimeOffsetFormat));
							break;
						case Guid guid:
							Bind(index, guid.ToString());
							break;
						default:
							throw new SQLiteException("Type not supported: " + value.GetType().Name);
				    }
					break;
			    default:
				    throw new ArgumentOutOfRangeException();
		    }
		}

	    public void Clear()
	    {
		    raw.sqlite3_clear_bindings(_stmt);
	    }

	    public void Reset()
	    {
		    raw.sqlite3_reset(_stmt);
	    }

		public void Dispose()
		{
			raw.sqlite3_finalize(_stmt);
		}
    }
}
