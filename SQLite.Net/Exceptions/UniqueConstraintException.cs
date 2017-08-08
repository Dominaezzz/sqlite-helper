namespace SQLite.Net.Exceptions
{
	public class UniqueConstraintException : SQLiteConstraintException
	{
		public UniqueConstraintException(string message) : base(message) { }
	}
}