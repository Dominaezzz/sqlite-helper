using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;
using SQLite.Net.Builders;

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
			type = Nullable.GetUnderlyingType(type) ?? type;
			return AllTypes.Contains(type) || type.GetTypeInfo().IsEnum;
		}

		public static string GetTableName(Type type)
		{
			return type.GetTypeInfo().GetCustomAttribute<TableAttribute>()?.Name ?? type.Name;
		}

		public static string GetViewName(Type type)
		{
			return type.GetTypeInfo().GetCustomAttribute<ViewAttribute>()?.Name ?? type.Name;
		}

		public static string GetColumnName(PropertyInfo property)
		{
			return property.GetCustomAttribute<ColumnAttribute>()?.Name ?? property.Name;
		}

		public static DataType GetDataType(Type columnType)
		{
			columnType = Nullable.GetUnderlyingType(columnType) ?? columnType;
			if (IntegerTypes.Contains(columnType))
			{
				return DataType.Integer;
			}
			else if (FractionalTypes.Contains(columnType))
			{
				return DataType.Real;
			}
			else if (TextTypes.Contains(columnType))
			{
				return DataType.Text;
			}
			else if (columnType == typeof(byte[]))
			{
				return DataType.Blob;
			}
			else if (columnType.GetTypeInfo().IsEnum)
			{
				return columnType.GetTypeInfo().IsDefined(typeof(StoreAsTextAttribute)) ? DataType.Text : DataType.Integer;
			}
			else
			{
				throw new ArgumentOutOfRangeException();
			}
		}
	}
}
