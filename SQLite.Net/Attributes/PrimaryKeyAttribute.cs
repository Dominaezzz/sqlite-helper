using System;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Property)]
    public sealed class PrimaryKeyAttribute : Attribute
	{
		public bool AutoIncrement { get; set; } = false;
	}
}
