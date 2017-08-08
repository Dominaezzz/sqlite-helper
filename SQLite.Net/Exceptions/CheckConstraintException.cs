namespace SQLite.Net.Exceptions
{
	public class CheckConstraintException : SQLiteConstraintException
	{
		public CheckConstraintException(string message) : base(message) { }
	}
}