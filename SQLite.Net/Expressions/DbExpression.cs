using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using SQLite.Net.Translation;

namespace SQLite.Net.Expressions
{
	internal enum DbExpressionType
	{
		Table = 1000, // make sure these don’t overlap with ExpressionType
		Column,
		Select,
		Projection,
		Join,
		Aggregate,
		In,
		Scalar,
		Exists,
		View,
		RawQuery,
		Function,
		TypeChange
	}

	internal class DbExpression : Expression
    {
	    public override string ToString()
	    {
		    return QueryFormatter.Format(this);
	    }
    }
}
