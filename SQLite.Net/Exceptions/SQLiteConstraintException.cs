namespace SQLite.Net.Exceptions
{
	public class SQLiteConstraintException : SQLiteException
	{
		public SQLiteConstraintException(string message) : base(message) { }
	}
}