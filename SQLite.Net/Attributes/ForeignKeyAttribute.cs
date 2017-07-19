using System;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Property)]
    public class ForeignKeyAttribute : Attribute
    {
	    public ForeignKeyAttribute(string table, string column)
	    {
		    Table = table;
		    Column = column;
	    }

	    public string Table { get; }
		public string Column { get; }
		public OnAction? OnDelete { get; set; }
		public OnAction? OnUpdate { get; set; }
    }
}
