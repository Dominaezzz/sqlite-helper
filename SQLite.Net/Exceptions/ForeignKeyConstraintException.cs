namespace SQLite.Net.Exceptions
{
	public class ForeignKeyConstraintException : SQLiteConstraintException
	{
		public ForeignKeyConstraintException(string message) : base(message) { }
	}
}