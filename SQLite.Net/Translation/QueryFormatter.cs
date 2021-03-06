﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using SQLite.Net.Attributes;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal class QueryFormatter : DbExpressionVisitor
	{
		protected enum Indentation { Same, Inner, Outer }

		internal const int IndentationWidth = 4;

		private readonly StringBuilder _sb = new StringBuilder();
		private readonly List<object> _args;
		private int _depth;

		private QueryFormatter(List<object> args)
		{
			_args = args;
		}

		internal static string Format(Expression expression, List<object> args = null)
		{
			QueryFormatter formatter = new QueryFormatter(args);
			formatter.Visit(expression);
			return formatter._sb.ToString();
		}

		private void AppendNewLine(Indentation style)
		{
			_sb.AppendLine();
			Indent(style);
			for (int i = 0, n = _depth * IndentationWidth; i < n; i++)
			{
				_sb.Append(' ');
			}
		}

		private void Indent(Indentation style)
		{
			if (style == Indentation.Inner)
			{
				_depth++;
			}
			else if (style == Indentation.Outer)
			{
				_depth--;
				System.Diagnostics.Debug.Assert(_depth >= 0);
			}
		}

		protected override Expression VisitMethodCall(MethodCallExpression call)
		{
			if (call.Method.DeclaringType == typeof(string))
			{
				switch (call.Method.Name)
				{
					case nameof(string.Equals):
						_sb.Append("(");
						Visit(call.Object);
						_sb.Append(" == (");
						Visit(call.Arguments[0]);
						_sb.Append("))");
						return call;
					case nameof(string.Concat):
						IList<Expression> args = call.Arguments;
						if (args.Count == 1 && args[0].NodeType == ExpressionType.NewArrayInit)
						{
							args = ((NewArrayExpression)args[0]).Expressions;
						}
						for (int i = 0, n = args.Count; i < n; i++)
						{
							if (i > 0) _sb.Append(" || ");
							Visit(args[i]);
						}
						return call;
					case nameof(string.Contains):
						Visit(call.Object);
						_sb.Append(" GLOB ('*' || ");
						Visit(call.Arguments[0]);
						_sb.Append(" || '*')");
						return call;
					case nameof(string.StartsWith):
						Visit(call.Object);
						_sb.Append(" GLOB (");
						Visit(call.Arguments[0]);
						_sb.Append(" || '*')");
						return call;
					case nameof(string.EndsWith):
						Visit(call.Object);
						_sb.Append(" GLOB ('*' || ");
						Visit(call.Arguments[0]);
						_sb.Append(")");
						return call;
					case nameof(string.ToLower):
						_sb.Append("LOWER(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
					case nameof(string.ToUpper):
						_sb.Append("UPPER(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
					case nameof(string.Replace):
						_sb.Append("REPLACE(");
						Visit(call.Object);
						_sb.Append(", ");
						Visit(call.Arguments[0]);
						_sb.Append(", ");
						Visit(call.Arguments[1]);
						_sb.Append(")");
						return call;
					case nameof(string.IsNullOrEmpty):
						_sb.Append("(IFNULL(");
						Visit(call.Arguments[0]);
						_sb.Append(", '') == '')");
						return call;
					case nameof(string.Substring):
						_sb.Append("SUBSTR(");
						Visit(call.Object);
						_sb.Append(", ");
						Visit(call.Arguments[0]);
						_sb.Append(" + 1");
						if (call.Arguments.Count == 2)
						{
							_sb.Append(", ");
							Visit(call.Arguments[1]);
						}
						_sb.Append(")");
						return call;
					case nameof(string.Remove):
						if (call.Arguments.Count == 1)
						{
							_sb.Append("SUBSTR(");
							Visit(call.Object);
							_sb.Append(", 1, ");
							Visit(call.Arguments[0]);
							_sb.Append(")");
							return call;
						}
						else
						{
							_sb.Append("(SUBSTR(");
							Visit(call.Object);
							_sb.Append(", 1, ");
							Visit(call.Arguments[0]);
							_sb.Append(") || SUBSTR(");
							Visit(call.Object);
							_sb.Append(", (");
							Visit(call.Arguments[0]);
							_sb.Append(" + ");
							Visit(call.Arguments[1]);
							_sb.Append(")))");
							return call;
						}
					case nameof(string.Trim):
						_sb.Append("TRIM(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
					case nameof(string.TrimStart):
						_sb.Append("LTRIM(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
					case nameof(string.TrimEnd):
						_sb.Append("RTRIM(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
				}
			}
			else if (call.Method.DeclaringType == typeof(Math))
			{
				switch (call.Method.Name)
				{
					case nameof(Math.Abs):
					case nameof(Math.Acos):
					case nameof(Math.Asin):
					case nameof(Math.Atan):
					case nameof(Math.Cos):
					case nameof(Math.Exp):
					case nameof(Math.Log10):
					case nameof(Math.Sin):
					case nameof(Math.Tan):
					case nameof(Math.Sqrt):
					case nameof(Math.Sign):
						_sb.Append(call.Method.Name.ToUpper());
						_sb.Append("(");
						Visit(call.Arguments[0]);
						_sb.Append(")");
						return call;
					case nameof(Math.Atan2):
					case nameof(Math.Pow):
					case nameof(Math.Min):
					case nameof(Math.Max):
						_sb.Append(call.Method.Name.ToUpper());
						_sb.Append("(");
						Visit(call.Arguments[0]);
						_sb.Append(", ");
						Visit(call.Arguments[1]);
						_sb.Append(")");
						return call;
					case nameof(Math.Log):
						if (call.Arguments.Count == 1)
						{
							goto case nameof(Math.Log10);
						}
						break;
					case nameof(Math.Round):
						if (call.Arguments.Count == 1)
						{
							_sb.Append("ROUND(");
							Visit(call.Arguments[0]);
							_sb.Append(")");
							return call;
						}
						else if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(int))
						{
							_sb.Append("ROUND(");
							Visit(call.Arguments[0]);
							_sb.Append(", ");
							Visit(call.Arguments[1]);
							_sb.Append(")");
							return call;
						}
						break;
				}
			}
			else if (call.Method.DeclaringType == typeof(decimal))
			{
				string GetOperator(string operatorName)
				{
					switch (operatorName)
					{
						case nameof(decimal.Add):
							return "+";
						case nameof(decimal.Subtract):
							return "-";
						case nameof(decimal.Multiply):
							return "*";
						case nameof(decimal.Divide):
							return "/";
						case nameof(decimal.Remainder):
							return "%";
						default:
							throw new ArgumentOutOfRangeException();
					}
				}

				switch (call.Method.Name)
				{
					case nameof(decimal.Add):
					case nameof(decimal.Subtract):
					case nameof(decimal.Multiply):
					case nameof(decimal.Divide):
					case nameof(decimal.Remainder):
						_sb.Append("(");
						Visit(call.Arguments[0]);
						_sb.Append(" ");
						_sb.Append(GetOperator(call.Method.Name));
						_sb.Append(" ");
						Visit(call.Arguments[1]);
						_sb.Append(")");
						return call;
					case nameof(decimal.Negate):
						_sb.Append("-");
						Visit(call.Arguments[0]);
						_sb.Append("");
						return call;
					case "Round":
						if (call.Arguments.Count == 1)
						{
							_sb.Append("ROUND(");
							Visit(call.Arguments[0]);
							_sb.Append(", 0)");
							return call;
						}
						else if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(int))
						{
							_sb.Append("ROUND(");
							Visit(call.Arguments[0]);
							_sb.Append(", ");
							Visit(call.Arguments[1]);
							_sb.Append(")");
							return call;
						}
						break;
				}
			}
			else if (call.Method.DeclaringType == typeof(DateTime))
			{
				Expression ApplyModifier(string type)
				{
					_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
					Visit(call.Object);
					_sb.Append(", ");
					Visit(call.Arguments[0]);
					_sb.Append(" || ' ").Append(type).Append("')");
					return call;
				}

				switch (call.Method.Name)
				{
					case nameof(DateTime.AddYears):
						return ApplyModifier("years");
					case nameof(DateTime.AddMonths):
						return ApplyModifier("months");
					case nameof(DateTime.AddDays):
						return ApplyModifier("days");
					case nameof(DateTime.AddHours):
						return ApplyModifier("hours");
					case nameof(DateTime.AddMinutes):
						return ApplyModifier("minutes");
					case nameof(DateTime.AddSeconds):
						return ApplyModifier("seconds");
					case nameof(DateTime.AddMilliseconds):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
						Visit(call.Object);
						_sb.Append(", (");
						Visit(call.Arguments[0]);
						_sb.Append(" / 1000.0) || ' seconds')");
						return call;
					case nameof(DateTime.AddTicks):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
						Visit(call.Object);
						_sb.Append(", (");
						Visit(call.Arguments[0]);
						_sb.AppendFormat(" / {0:F1}) || ' seconds')", TimeSpan.TicksPerSecond);
						return call;
					case nameof(DateTime.Add):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
						Visit(call.Object);
						_sb.Append(", (");
						Visit(call.Arguments[0]);
						_sb.AppendFormat(" / {0:F1}) || ' seconds')", TimeSpan.TicksPerSecond);
						return call;
					case nameof(DateTime.Subtract):
						if (call.Arguments[1].Type == typeof(DateTime))
						{
							_sb.Append("ROUND(");
							{
								_sb.Append("(JULIANDAY(");
								Visit(call.Arguments[0]);
								_sb.Append(") - JULIANDAY(");
								Visit(call.Arguments[1]);
								_sb.Append("))");
							}
							_sb.AppendFormat(" * {0:D})", TimeSpan.TicksPerDay);
							return call;
						}
						else if (call.Arguments[1].Type == typeof(TimeSpan))
						{
							_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
							Visit(call.Object);
							_sb.Append(", -(");
							Visit(call.Arguments[0]);
							_sb.AppendFormat(" / {0:F1}) || ' seconds')", TimeSpan.TicksPerSecond);
							return call;
						}
						break;
				}
			}
			else if (call.Method.DeclaringType == typeof(TimeSpan))
			{
				switch (call.Method.Name)
				{
					case nameof(TimeSpan.FromDays):
						_sb.Append("ROUND(");
						Visit(call.Arguments[0]);
						_sb.Append(" * ");
						_sb.Append(TimeSpan.TicksPerDay);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.FromHours):
						_sb.Append("ROUND(");
						Visit(call.Arguments[0]);
						_sb.Append(" * ");
						_sb.Append(TimeSpan.TicksPerHour);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.FromMinutes):
						_sb.Append("ROUND(");
						Visit(call.Arguments[0]);
						_sb.Append(" * ");
						_sb.Append(TimeSpan.TicksPerMinute);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.FromSeconds):
						_sb.Append("ROUND(");
						Visit(call.Arguments[0]);
						_sb.Append(" * ");
						_sb.Append(TimeSpan.TicksPerSecond);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.FromMilliseconds):
						_sb.Append("ROUND(");
						Visit(call.Arguments[0]);
						_sb.Append(" * ");
						_sb.Append(TimeSpan.TicksPerMillisecond);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.FromTicks):
						return Visit(call.Arguments[0]);
					case nameof(TimeSpan.Add):
						_sb.Append("(");
						Visit(call.Object);
						_sb.Append(" + ");
						Visit(call.Arguments[0]);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.Subtract):
						_sb.Append("(");
						Visit(call.Object);
						_sb.Append(" - ");
						Visit(call.Arguments[0]);
						_sb.Append(")");
						return call;
					case nameof(TimeSpan.Negate):
						_sb.Append("-");
						Visit(call.Object);
						return call;
					case nameof(TimeSpan.Duration):
						_sb.Append("ABS(");
						Visit(call.Object);
						_sb.Append(")");
						return call;
				}
			}
			if (call.Method.Name == nameof(ToString))
			{
				// no-op
				Visit(call.Object);
				return call;
			}
			throw new NotSupportedException($"The method ‘{call.Method.Name}’ is not supported");
		}

		protected override Expression VisitMember(MemberExpression m)
		{
			if (m.Member.DeclaringType == typeof(string))
			{
				switch (m.Member.Name)
				{
					case nameof(string.Length):
						_sb.Append("LENGTH(");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
				}
			}
			else if (m.Member.DeclaringType == typeof(DateTime) || m.Member.DeclaringType == typeof(DateTimeOffset))
			{
				switch (m.Member.Name)
				{
					case nameof(DateTime.Year):
						_sb.Append("STRFTIME('%Y', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Month):
						_sb.Append("STRFTIME('%m', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Day):
						_sb.Append("STRFTIME('%d', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Hour):
						_sb.Append("STRFTIME('%H', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Minute):
						_sb.Append("STRFTIME('%M', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Second):
						_sb.Append("STRFTIME('%S', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Millisecond):
						_sb.Append("CAST(SUBSTR(STRFTIME('%f', ");
						Visit(m.Expression);
						_sb.Append("), 4) AS INTEGER)");
						return m;
					case nameof(DateTime.DayOfYear):
						_sb.Append("STRFTIME('%j', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.DayOfWeek):
						_sb.Append("STRFTIME('%w', ");
						Visit(m.Expression);
						_sb.Append(")");
						return m;
					case nameof(DateTime.Date):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', ");
						Visit(m.Expression);
						_sb.Append(", 'start of day')");
						return m;
					case nameof(DateTime.TimeOfDay):
						_sb.Append("(ROUND(");
						{
							_sb.Append("(JULIANDAY(");
							Visit(m.Expression);
							_sb.Append(") - JULIANDAY(");
							Visit(m.Expression);
							_sb.Append(", 'start of day'))");
						}
						_sb.AppendFormat(
							" * {0:D}) * {1:D})",
							TimeSpan.TicksPerDay / TimeSpan.TicksPerMillisecond,
							TimeSpan.TicksPerMillisecond
						);
						return m;
					case nameof(DateTime.Today):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', 'now', 'start of day')");
						return m;
					case nameof(DateTime.Now):
						_sb.Append("STRFTIME('" + Orm.DateTimeSqlFormat + "', 'now')");
						return m;
				}
			}
			else if (m.Member.DeclaringType == typeof(TimeSpan))
			{
				switch (m.Member.Name)
				{
					case nameof(TimeSpan.TotalDays):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:F1})", TimeSpan.TicksPerDay);
						return m;
					case nameof(TimeSpan.TotalHours):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:F1})", TimeSpan.TicksPerHour);
						return m;
					case nameof(TimeSpan.TotalMinutes):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:F1})", TimeSpan.TicksPerMinute);
						return m;
					case nameof(TimeSpan.TotalSeconds):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:F1})", TimeSpan.TicksPerSecond);
						return m;
					case nameof(TimeSpan.TotalMilliseconds):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:F1})", TimeSpan.TicksPerMillisecond);
						return m;
					case nameof(TimeSpan.Ticks):
						Visit(m.Expression);
						return m;
					case nameof(TimeSpan.Days):
						_sb.Append("(");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:D})", TimeSpan.TicksPerDay);
						return m;
					case nameof(TimeSpan.Hours):
						_sb.Append("((");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:D}) % 24)", TimeSpan.TicksPerHour);
						return m;
					case nameof(TimeSpan.Minutes):
						_sb.Append("((");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:D}) % 60)", TimeSpan.TicksPerMinute);
						return m;
					case nameof(TimeSpan.Seconds):
						_sb.Append("((");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:D}) % 60)", TimeSpan.TicksPerSecond);
						return m;
					case nameof(TimeSpan.Milliseconds):
						_sb.Append("((");
						Visit(m.Expression);
						_sb.AppendFormat(" / {0:D}) % 1000)", TimeSpan.TicksPerMillisecond);
						return m;
				}
			}
			return base.VisitMember(m);
		}

		protected override Expression VisitUnary(UnaryExpression u)
		{
			switch (u.NodeType)
			{
				case ExpressionType.Not:
					_sb.Append(" NOT ");
					Visit(u.Operand);
					break;
				case ExpressionType.Negate:
					_sb.Append("-");
					Visit(u.Operand);
					break;
				case ExpressionType.Convert:
					if (Nullable.GetUnderlyingType(u.Type) == u.Operand.Type)
					{
						Visit(u.Operand);
						break;
					}
					else if (u.Operand.Type.GetTypeInfo().IsValueType && u.Type.GetTypeInfo().IsValueType)
					{
						Visit(u.Operand);
						break;
					}
					else if (u.Type == typeof(object))
					{
						Visit(u.Operand);
						break;
					}
					goto default;
				default:
					throw new NotSupportedException($"The unary operator ‘{u.NodeType}’ is not supported");
			}
			return u;
		}

		protected override Expression VisitBinary(BinaryExpression b)
		{
			int index = _sb.Length - 1;
			_sb.Append("(");
			Visit(b.Left);
			switch (b.NodeType)
			{
				case ExpressionType.Add: case ExpressionType.AddChecked:
					_sb.Append(b.Left.Type == typeof(string) ? " || " : " + ");
					break;
				case ExpressionType.Subtract: case ExpressionType.SubtractChecked:
					_sb.Append(" - ");
					break;
				case ExpressionType.Modulo:
					_sb.Append(" % ");
					break;
				case ExpressionType.Multiply: case ExpressionType.MultiplyChecked:
					_sb.Append(" * ");
					break;
				case ExpressionType.Divide:
					_sb.Append(" / ");
					break;
				case ExpressionType.And:
					_sb.Append(" & ");
					break;
				case ExpressionType.Or:
					_sb.Append(" | ");
					break;
				case ExpressionType.ExclusiveOr:
					_sb.Append(" ^ ");
					break;
				case ExpressionType.LeftShift:
					_sb.Append(" << ");
					break;
				case ExpressionType.RightShift:
					_sb.Append(" >> ");
					break;
				case ExpressionType.AndAlso:
					_sb.Append(" AND ");
					break;
				case ExpressionType.OrElse:
					_sb.Append(" OR ");
					break;
				case ExpressionType.Equal:
					_sb.Append(" IS ");
					break;
				case ExpressionType.NotEqual:
					_sb.Append(" IS NOT ");
					break;
				case ExpressionType.LessThan:
					_sb.Append(" < ");
					break;
				case ExpressionType.LessThanOrEqual:
					_sb.Append(" <= ");
					break;
				case ExpressionType.GreaterThan:
					_sb.Append(" > ");
					break;
				case ExpressionType.GreaterThanOrEqual:
					_sb.Append(" >= ");
					break;
				case ExpressionType.Coalesce:
					_sb.Insert(index, "IFNULL");
					_sb.Append(", ");
					break;
				default:
					throw new NotSupportedException($"The binary operator ‘{b.NodeType}’ is not supported");
			}
			Visit(b.Right);
			_sb.Append(")");
			return b;
		}

		protected override Expression VisitConditional(ConditionalExpression c)
		{
			if (IsPredicate(c.Test))
			{
				_sb.Append("(CASE WHEN ");
				Visit(c.Test);
				_sb.Append(" THEN ");
				Visit(c.IfTrue);
				Expression ifFalse = c.IfFalse;
				while (ifFalse != null && ifFalse.NodeType == ExpressionType.Conditional)
				{
					ConditionalExpression fc = (ConditionalExpression)ifFalse;
					_sb.Append(" WHEN ");
					Visit(fc.Test);
					_sb.Append(" THEN ");
					Visit(fc.IfTrue);
					ifFalse = fc.IfFalse;
				}
				if (ifFalse != null)
				{
					_sb.Append(" ELSE ");
					Visit(ifFalse);
				}
				_sb.Append(" END)");
			}
			else
			{
				_sb.Append("(CASE ");
				Visit(c.Test);
				_sb.Append(" WHEN 0 THEN ");
				Visit(c.IfFalse);
				_sb.Append(" ELSE ");
				Visit(c.IfTrue);
				_sb.Append(" END)");
			}
			return c;
		}

		protected override Expression VisitConstant(ConstantExpression c)
		{
			switch (Convert.GetTypeCode(c.Value))
			{
				case TypeCode.Empty:
					_sb.Append("NULL");
					break;
				case TypeCode.Boolean:
					_sb.Append(((bool)c.Value) ? 1 : 0);
					break;
				case TypeCode.String:
					_sb.Append("'").Append(c.Value).Append("'");
					break;
				case TypeCode.DateTime:
					_sb.Append('\'')
						.Append(((DateTime)c.Value).ToString(Orm.DateTimeFormat))
						.Append('\'');
					break;
				case TypeCode.Decimal:case TypeCode.Double:case TypeCode.Single:
					_sb.AppendFormat("{0:0.0###########}", c.Value);
					break;
				case TypeCode.Object:
					if (c.Value.GetType().GetTypeInfo().IsEnum)
					{
						if (c.Value.GetType().GetTypeInfo().IsDefined(typeof(StoreAsTextAttribute)))
						{
							_sb.Append('\'').Append(c.Value).Append('\'');
						}
						else
						{
							_sb.Append(Array.IndexOf(Enum.GetValues(c.Value.GetType()), c.Value));
						}
					}
					switch (c.Value)
					{
						case TimeSpan timeSpan:
							_sb.Append(timeSpan.Ticks);
							break;
						case Guid guid:
							_sb.Append("'").Append(guid).Append("'");
							break;
						case DateTimeOffset dateTimeOffset:
							_sb.Append('\'')
								.Append(dateTimeOffset.ToString(Orm.DateTimeOffsetFormat))
								.Append('\'');
							break;
						default:
							throw new NotSupportedException($"The constant for ‘{c.Value}’ is not supported");
					}
					break;
				default:
					_sb.Append(c.Value);
					break;
			}
			return c;
		}

		protected override Expression VisitHostParameter(HostParameterExpression hostParameter)
		{
			if (_args != null)
			{
				_args.Add(hostParameter.Value);
				_sb.Append("?");
			}
			else
			{
				return Visit(Expression.Constant(hostParameter.Value, hostParameter.Type));
			}
			return base.VisitHostParameter(hostParameter);
		}

		protected override Expression VisitColumn(ColumnExpression column)
		{
			if (!string.IsNullOrEmpty(column.Alias))
			{
				_sb.Append(column.Alias).Append(".");
			}
			_sb.Append('[').Append(column.Name).Append(']');
			return column;
		}

		protected override Expression VisitFunction(FunctionExpression function)
		{
			_sb.Append(function.Name);
			_sb.Append("(");
			for (int i = 0; i < function.Arguments.Count; i++)
			{
				if (i > 0) _sb.Append(", ");

				Visit(function.Arguments[i]);
			}
			_sb.Append(")");
			return function;
		}

		protected override Expression VisitSelect(SelectExpression select)
		{
			_sb.Append("SELECT ");
			if (select.IsDistinct)
			{
				_sb.Append("DISTINCT ");
			}
			for (int i = 0; i < select.Columns.Count; i++)
			{
				ColumnDeclaration column = select.Columns[i];
				if (i > 0) _sb.Append(", ");

				ColumnExpression c = Visit(column.Expression) as ColumnExpression;

				if (!(string.IsNullOrWhiteSpace(column.Name) || string.Equals(c?.Name, column.Name)))
				{
					_sb.Append(" AS ").Append(column.Name);
				}
			}

			if (select.From != null)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("FROM ");
				VisitSource(select.From);
			}
			if (select.Where != null)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("WHERE ");
				Visit(select.Where);
			}
			if (select.GroupBy != null && select.GroupBy.Count > 0)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("GROUP BY ");
				for (int i = 0, n = select.GroupBy.Count; i < n; i++)
				{
					if (i > 0) _sb.Append(", ");
					Visit(select.GroupBy[i]);
				}
			}
			if (select.Having != null)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("HAVING ");
				Visit(select.Having);
			}
			if (select.OrderBy != null && select.OrderBy.Count > 0)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("ORDER BY ");
				for (int i = 0, n = select.OrderBy.Count; i < n; i++)
				{
					if (i > 0) _sb.Append(", ");
					OrderExpression exp = select.OrderBy[i];
					Visit(exp.Expression);
					_sb.Append(exp.OrderType != OrderType.Ascending ? " DESC" : " ASC");
				}
			}
			if (select.Limit != null)
			{
				AppendNewLine(Indentation.Same);
				_sb.Append("LIMIT ");
				Visit(select.Limit);
			}
			if (select.Offset != null)
			{
				if (select.Limit == null)
				{
					AppendNewLine(Indentation.Same);
					_sb.Append("LIMIT -1");
				}
				_sb.Append(" OFFSET ");
				Visit(select.Offset);
			}
			return select;
		}

		protected override Expression VisitRawQuery(RawQueryExpression rawQuery)
		{
			_sb.Append(rawQuery.SQLQuery);
			return rawQuery;
		}

		protected override Expression VisitSource(Expression source)
		{
			switch ((DbExpressionType)source.NodeType)
			{
				case DbExpressionType.Table:
					TableExpression table = (TableExpression)source;
					_sb.Append('[')
						.Append(table.Name)
						.Append(']');
					if (!string.IsNullOrWhiteSpace(table.Alias)) _sb.Append(" AS ").Append(table.Alias);
					break;
				case DbExpressionType.RawQuery:
					RawQueryExpression rawQuery = (RawQueryExpression)source;
					_sb.Append("(")
						.Append(rawQuery.SQLQuery)
						.Append(")");
					if (!string.IsNullOrWhiteSpace(rawQuery.Alias)) _sb.Append(" AS ").Append(rawQuery.Alias);
					break;
				case DbExpressionType.Select:
					SelectExpression select = (SelectExpression)source;
					_sb.Append("(");
					AppendNewLine(Indentation.Inner);
					Visit(select);
					AppendNewLine(Indentation.Outer);
					_sb.Append(")");
					if (!string.IsNullOrWhiteSpace(select.Alias)) _sb.Append(" AS ").Append(select.Alias);
					break;
				case DbExpressionType.Join:
					VisitJoin((JoinExpression)source);
					break;
				default:
					throw new InvalidOperationException("Select source is not valid type");
			}
			return source;
		}

		protected override Expression VisitJoin(JoinExpression join)
		{
			VisitSource(join.Left);
			AppendNewLine(Indentation.Same);

			switch (join.Join)
			{
				case JoinType.CrossJoin:
					_sb.Append("CROSS JOIN ");
					break;
				case JoinType.InnerJoin:
					_sb.Append("INNER JOIN ");
					break;
				case JoinType.OuterJoin:
					_sb.Append("OUTER JOIN ");
					break;
			}

			VisitSource(join.Right);

			if (join.Condition != null)
			{
				AppendNewLine(Indentation.Inner);
				_sb.Append("ON ");
				Visit(join.Condition);
				Indent(Indentation.Outer);
			}
			return join;
		}

		private static string GetAggregateName(AggregateType aggregateType)
		{
			switch (aggregateType)
			{
				case AggregateType.Count: return "COUNT";
				case AggregateType.Min: return "MIN";
				case AggregateType.Max: return "MAX";
				case AggregateType.Sum: return "SUM";
				case AggregateType.Average: return "AVG";
				default: throw new Exception($"Unknown aggregate type: {aggregateType}");
			}
		}
		
		protected override Expression VisitAggregate(AggregateExpression aggregate)
		{
			_sb.Append(GetAggregateName(aggregate.AggregateType));
			_sb.Append("(");
			if (aggregate.Argument != null)
			{
				Visit(aggregate.Argument);
			}
			else if (aggregate.AggregateType == AggregateType.Count)
			{
				_sb.Append("*");
			}
			_sb.Append(")");
			return aggregate;
		}

		protected override Expression VisitScalar(ScalarExpression scalar)
		{
			_sb.Append("(");
			AppendNewLine(Indentation.Inner);
			Visit(scalar.Query);
			AppendNewLine(Indentation.Same);
			_sb.Append(")");
			Indent(Indentation.Outer);
			return scalar;
		}

		protected override Expression VisitExists(ExistsExpression exists)
		{
			_sb.Append("EXISTS(");
			AppendNewLine(Indentation.Inner);
			Visit(exists.Query);
			AppendNewLine(Indentation.Same);
			_sb.Append(")");
			Indent(Indentation.Outer);
			return exists;
		}

		protected override Expression VisitIn(InExpression inExpression)
		{
			Visit(inExpression.Expression);
			_sb.Append(" IN (");
			AppendNewLine(Indentation.Inner);
			if (inExpression.Query != null)
			{
				Visit(inExpression.Query);
			}
			else
			{
				for (int i = 0; i < inExpression.Values.Count; i++)
				{
					if (i > 0) _sb.Append(", ");
					Visit(inExpression.Values[i]);
				}
			}
			AppendNewLine(Indentation.Same);
			_sb.Append(")");
			Indent(Indentation.Outer);
			return inExpression;
		}

		protected virtual bool IsPredicate(Expression expr)
		{
			switch (expr.NodeType)
			{
				case ExpressionType.And:
				case ExpressionType.AndAlso:
				case ExpressionType.OrElse:
				case ExpressionType.Not:
				case ExpressionType.Equal:
				case ExpressionType.NotEqual:
				case ExpressionType.LessThan:
				case ExpressionType.LessThanOrEqual:
				case ExpressionType.GreaterThan:
				case ExpressionType.GreaterThanOrEqual:
//				case (ExpressionType)DbExpressionType.Between:
				case (ExpressionType)DbExpressionType.Exists:
				case (ExpressionType)DbExpressionType.In:
					return true;
				case ExpressionType.Call:
					return ((MethodCallExpression)expr).Type == typeof(bool);
				default:
					return false;
			}
		}
	}
}
