namespace SQLite.Net.Exceptions
{
	public class NotNullConstraintException : SQLiteConstraintException
	{
		public NotNullConstraintException(string message) : base(message) { }
	}
}