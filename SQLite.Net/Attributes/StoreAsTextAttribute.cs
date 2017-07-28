using System;
using System.Collections.Generic;
using System.Text;

namespace SQLite.Net.Attributes
{
	/// <summary>
	/// By default enums are interpreted as integers,
	/// if this attibute is applied to the Enum declaration then it is stored as text.
	/// </summary>
	[AttributeUsage(AttributeTargets.Enum)]
    public class StoreAsTextAttribute : Attribute
    {
    }
}
