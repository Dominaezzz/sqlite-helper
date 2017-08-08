using System;

namespace SQLite.Net.Exceptions
{
	public class SQLiteException : Exception
	{
		public SQLiteException() { }

		public SQLiteException(string message) : base(message) { }
	}
}
