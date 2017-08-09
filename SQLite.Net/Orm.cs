using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;

namespace SQLite.Net
{
    internal class Orm
	{
		public static readonly DateTime Unixepoch = new DateTime(1970, 1, 1);
		public const string DateFormat = "yyyy-MM-dd";
		public const string TimeFormat = "HH:mm:ss.fff";
		public const string DateTimeFormat = DateFormat + " " + TimeFormat;
		public const string DateTimeOffsetFormat = DateTimeFormat + " zzzz";
		public const string DateSqlFormat = "%Y-%m-%d";
		public const string TimeSqlFormat = "%H:%M:%f";
		public const string DateTimeSqlFormat = DateSqlFormat + " " + TimeSqlFormat;

		public static readonly HashSet<Type> AllTypes = new HashSet<Type>
		{
			typeof(bool),

			typeof(byte), typeof(sbyte),
			typeof(ushort), typeof(short),
			typeof(int), typeof(uint),
			typeof(long), typeof(ulong),

			typeof(float), typeof(double), typeof(decimal),

			typeof(string), typeof(char),

			typeof(byte[]),

			typeof(DateTime), typeof(DateTimeOffset),
			typeof(TimeSpan),

			typeof(Guid)
		};

		public static readonly HashSet<Type> IntegralTypes = new HashSet<Type>
		{
			typeof(byte), typeof(sbyte),
			typeof(ushort), typeof(short),
			typeof(int), typeof(uint),
			typeof(long), typeof(ulong),
		};

		public static readonly HashSet<Type> FractionalTypes = new HashSet<Type>
		{
			typeof(float), typeof(double), typeof(decimal)
		};


		public static readonly HashSet<Type> IntegerTypes = new HashSet<Type>
		{
			typeof(bool), typeof(TimeSpan),
			typeof(byte), typeof(sbyte),
			typeof(ushort), typeof(short),
			typeof(int), typeof(uint),
			typeof(long), typeof(ulong),
		};
		
		public static readonly HashSet<Type> TextTypes = new HashSet<Type>
		{
			typeof(string), typeof(char),
			typeof(DateTime), typeof(DateTimeOffset),
			typeof(Guid)
		};

		public static bool IsColumnTypeSupported(Type type)
		{
			return AllTypes.Contains(type) || type.GetTypeInfo().IsEnum;
		}

		public static string GetColumnName(PropertyInfo property)
		{
			return property.GetCustomAttribute<ColumnAttribute>()?.Name ?? property.Name;
		}
	}
}
