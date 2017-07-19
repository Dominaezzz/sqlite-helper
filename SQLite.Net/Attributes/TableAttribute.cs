using System;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
    public sealed class TableAttribute : Attribute
    {
	    public TableAttribute(string name)
	    {
		    Name = name;
	    }

	    public string Name { get; }
    }
}
