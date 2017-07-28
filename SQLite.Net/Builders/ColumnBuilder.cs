using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;

namespace SQLite.Net.Builders
{
	public enum DataType
	{
		Integer,
		Real,
		Text,
		Blob
	}

	public class ColumnModel
	{
		public ColumnModel(string name, DataType type, bool nullable, object @default, bool primaryKey, bool autoIncrement)
		{
			Name = name;
			Type = type;
			Nullable = nullable;
			Default = @default;
			PrimaryKey = primaryKey;
			AutoIncrement = autoIncrement;
		}

		public string Name { get; set; }
		public DataType Type { get; }
		public bool Nullable { get; }
		public object Default { get; }

		public bool PrimaryKey { get; }
		public bool AutoIncrement { get; }
	}

    public class ColumnBuilder
    {
		internal ColumnBuilder() { }

	    public ColumnModel Column<T>(string name = null, bool? nullable = null, object defaultValue = null, bool primaryKey = false, bool autoIncrement = false)
	    {
			Type columnType = typeof(T);
		    Type underlyingType = Nullable.GetUnderlyingType(columnType);
			
		    if (underlyingType != null)
		    {
			    nullable = nullable ?? true;
			    columnType = underlyingType;
		    }
		    else if (!columnType.GetTypeInfo().IsValueType)
		    {
			    nullable = nullable ?? true;
		    }
		    else
		    {
			    nullable = nullable ?? false;
		    }

		    DataType type;
		    if (Orm.IntegerTypes.Contains(columnType))
		    {
			    type = DataType.Integer;
		    }
		    else if (Orm.FractionalTypes.Contains(columnType))
		    {
			    type = DataType.Real;
		    }
		    else if (Orm.TextTypes.Contains(columnType))
		    {
			    type = DataType.Text;
		    }
		    else if (columnType == typeof(byte[]))
		    {
			    type = DataType.Blob;
		    }
			else if (columnType.GetTypeInfo().IsEnum)
		    {
			    type = columnType.GetTypeInfo().IsDefined(typeof(StoreAsTextAttribute)) ? DataType.Text : DataType.Integer;
		    }
		    else
		    {
			    throw new ArgumentOutOfRangeException();
		    }

		    return new ColumnModel(name, type, nullable.Value, defaultValue, primaryKey, autoIncrement);
		}
	}
}
