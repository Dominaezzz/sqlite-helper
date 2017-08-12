using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal sealed class ProjectedColumns
	{
		public ProjectedColumns(Expression projector, ReadOnlyCollection<ColumnDeclaration> columns)
		{
			Projector = projector;
			Columns = columns;
		}

		public Expression Projector { get; }
		public ReadOnlyCollection<ColumnDeclaration> Columns { get; }
	}

	internal class ColumnProjector : DbExpressionVisitor
	{
		private readonly Dictionary<ColumnExpression, ColumnExpression> _map = new Dictionary<ColumnExpression, ColumnExpression>();
		private readonly List<ColumnDeclaration> _columns = new List<ColumnDeclaration>();
		private readonly HashSet<string> _columnNames = new HashSet<string>();

		private readonly HashSet<Expression> _candidates;

		private readonly string _newAlias;
		private int _columnIndex;

		internal ColumnProjector(Expression expression, string newAlias, IEnumerable<string> existingAliases)
		{
			_candidates = Nominator.Nominate(existingAliases, expression);
			_newAlias = newAlias;
		}
		
		internal static ProjectedColumns ProjectColumns(Expression expression, string newAlias, IEnumerable<string> existingAliases)
		{
			ColumnProjector projector = new ColumnProjector(expression, newAlias, existingAliases);
			return new ProjectedColumns(projector.Visit(expression), projector._columns.AsReadOnly());
		}
		
		public override Expression Visit(Expression expression)
		{
			if (_candidates.Contains(expression))
			{
				if (expression.NodeType == (ExpressionType)DbExpressionType.Column)
				{
					ColumnExpression column = (ColumnExpression)expression;
					ColumnExpression mapped;
					// If column has been mapped already just return it.
					if (_map.TryGetValue(column, out mapped)) return mapped;
					
					string columnName = GetUniqueColumnName(column.Name);

					_columns.Add(new ColumnDeclaration(columnName, column));
					_columnNames.Add(columnName);
					
					_map[column] = mapped = new ColumnExpression(column.Type, _newAlias, columnName);
					return mapped;
				}
				else
				{
					string columnName = GetNextColumnName();
					_columns.Add(new ColumnDeclaration(columnName, expression));
					return new ColumnExpression(expression.Type, _newAlias, columnName);
				}
			}
			return base.Visit(expression);
		}

		private bool IsColumnNameInUse(string name)
		{
			return _columnNames.Contains(name);
		}

		private string GetUniqueColumnName(string name)
		{
			string baseName = name;
			int suffix = 1;

			while (IsColumnNameInUse(name))
			{
				name = $"{baseName}{suffix++}";
			}
			return name;
		}

		private string GetNextColumnName()
		{
			return GetUniqueColumnName($"c{_columnIndex++}");
		}

		/// <summary>
		/// Nominator is a class that walks an expression tree bottom up, determining the set of 
		/// candidate expressions that are possible columns of a select expression
		/// </summary>
		private class Nominator : DbExpressionVisitor
		{
			private readonly List<string> _existingAliases;
			private readonly HashSet<Expression> _candidates = new HashSet<Expression>();
			private bool _isBlocked = false;

			private Nominator(IEnumerable<string> existingAliases)
			{
				_existingAliases = existingAliases?.ToList() ?? new List<string>();
			}

			internal static HashSet<Expression> Nominate(IEnumerable<string> existingAliases, Expression expression)
			{
				Nominator nominator = new Nominator(existingAliases);
				nominator.Visit(expression);
				return nominator._candidates;
			}

			/// <summary>
			/// Determines whether the given expression can be represented as a column in a select expressionss
			/// </summary>
			/// <param name="expression"></param>
			/// <returns></returns>
			private bool CanBeColumn(Expression expression)
			{
				switch (expression.NodeType)
				{
					case ExpressionType.Add:
					case ExpressionType.AddChecked:
					case ExpressionType.Subtract:
					case ExpressionType.SubtractChecked:
					case ExpressionType.Multiply:
					case ExpressionType.MultiplyChecked:
					case ExpressionType.Divide:
					case ExpressionType.Modulo:
					case ExpressionType.And:
					case ExpressionType.AndAlso:
					case ExpressionType.Or:
					case ExpressionType.OrElse:
					case ExpressionType.ExclusiveOr:
					case ExpressionType.OnesComplement:
					case ExpressionType.Not:
					case ExpressionType.Negate:
					case ExpressionType.NegateChecked:
					case ExpressionType.UnaryPlus:
					case ExpressionType.RightShift:
					case ExpressionType.LeftShift:
					case ExpressionType.Equal:
					case ExpressionType.NotEqual:
					case ExpressionType.GreaterThan:
					case ExpressionType.GreaterThanOrEqual:
					case ExpressionType.LessThan:
					case ExpressionType.LessThanOrEqual:
					case ExpressionType.Coalesce:
						return true;
					case ExpressionType.Constant:
						return true;
					case ExpressionType.Convert:
					case ExpressionType.ConvertChecked:
						return true;
					case ExpressionType.Conditional:
						return true;
					case ExpressionType.Call:
						Type declaringType = ((MethodCallExpression) expression).Method.DeclaringType;
						if (declaringType == typeof(DateTime) || declaringType == typeof(DateTimeOffset) ||
							declaringType == typeof(TimeSpan) || declaringType == typeof(string) ||
							declaringType == typeof(decimal)  || declaringType == typeof(Math))
						{
							return true;
						}
						goto default;
					case ExpressionType.MemberAccess:
						Type mDeclaringType = ((MemberExpression) expression).Member.DeclaringType;
						if (mDeclaringType == typeof(DateTime) || mDeclaringType == typeof(DateTimeOffset) ||
							mDeclaringType == typeof(TimeSpan) || mDeclaringType == typeof(string))
						{
							return true;
						}
						goto default;
					default:
						return MustBeColumn(expression);
				}
			}

			private bool MustBeColumn(Expression expression)
			{
				switch (expression.NodeType)
				{
					case (ExpressionType)DbExpressionType.Column:
						ColumnExpression column = (ColumnExpression) expression;
						// If the column does not belong to an immediately existing alias
						// then it must be referring to outer scope.
						if (_existingAliases.Contains(column.Alias))
						{
							return true;
						}
						goto default;
					case (ExpressionType)DbExpressionType.Scalar:
					case (ExpressionType)DbExpressionType.Aggregate:
					case (ExpressionType)DbExpressionType.Function:
					case (ExpressionType)DbExpressionType.HostParameter:
						return true;
					default:
						return false;
				}
			}

			public override Expression Visit(Expression expression)
			{
				if (expression == null) return null;

				bool saveIsBlocked = _isBlocked;
				if (MustBeColumn(expression))
				{
					_candidates.Add(expression);
					_isBlocked = false;
				}
				else
				{
					base.Visit(expression);
					if (!_isBlocked)
					{
						if (CanBeColumn(expression))
						{
							_candidates.Add(expression);
							_isBlocked = false;
						}
						else
						{
							_isBlocked = true;
						}
					}
				}
				_isBlocked |= saveIsBlocked;
				return expression;
			}
		}
	}
}
