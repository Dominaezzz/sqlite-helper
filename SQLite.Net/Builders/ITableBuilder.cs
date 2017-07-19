using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLite.Net.Builders
{
    public interface ITableBuilder<out T>
    {
	    TColumns PrimaryKey<TColumns>(Func<T, TColumns> columns);

	    TColumns Unique<TColumns>(Func<T, TColumns> columns);

	    TColumns ForeignKey<TColumns>(Func<T, TColumns> columns, string foreignTable, params string[] foreignColumns);
    }
}
