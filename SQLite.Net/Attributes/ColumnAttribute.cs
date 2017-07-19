using System;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Property)]
    public sealed class ColumnAttribute : Attribute
    {
	    public ColumnAttribute(string name)
	    {
		    Name = name;
	    }

	    public string Name { get; }
    }
}
