using System;

namespace SQLite.Net.Attributes
{
	[AttributeUsage(AttributeTargets.Property)]
    public sealed class NotNullAttribute : Attribute
    {
    }
}
