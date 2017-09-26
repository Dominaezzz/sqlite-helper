using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Class)]
    public sealed class ViewAttribute : Attribute
    {
	    public ViewAttribute(string name)
	    {
		    Name = name;
	    }

	    public string Name { get; }
    }
}
