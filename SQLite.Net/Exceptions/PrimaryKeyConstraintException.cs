namespace SQLite.Net.Exceptions
{
	public class PrimaryKeyConstraintException : SQLiteConstraintException
	{
		public PrimaryKeyConstraintException(string message) : base(message) { }
	}
}