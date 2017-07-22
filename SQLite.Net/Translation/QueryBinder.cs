using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using SQLite.Net.Attributes;
using SQLite.Net.Expressions;
using SQLite.Net.Helpers;

namespace SQLite.Net.Translation
{
	internal class QueryBinder : DbExpressionVisitor
	{
		/// <summary>
		/// Dictionary of lambda parameters to bodies for replacing.
		/// </summary>
		private readonly Dictionary<ParameterExpression, Expression> _map = new Dictionary<ParameterExpression, Expression>();

		private readonly IQueryProvider _provider;
		private Expression _root;
		private uint _aliasCount;
		private List<OrderExpression> _thenBys;

		private QueryBinder(IQueryProvider provider, Expression root)
		{
			_provider = provider;
			_root = root;
		}

		public static Expression Bind(IQueryProvider provider, Expression root)
		{
			return new QueryBinder(provider, root).Visit(root);
		}
		
		private static LambdaExpression GetLambda(Expression e)
		{
			while (e.NodeType == ExpressionType.Quote)
			{
				e = ((UnaryExpression)e).Operand;
			}
			return (LambdaExpression) e;
		}

		private string GetNewAlias()
		{
			return $"t{_aliasCount++}";
		}

		private static string GetExistingAlias(Expression source)
		{
			if (source is AliasedExpression aliasedExpression)
			{
				return aliasedExpression.Alias;
			}
			throw new InvalidOperationException($"Invalid source node type ‘{source.NodeType}'");
		}

		private static ProjectedColumns ProjectColumns(Expression expression, string newAlias, params string[] existingAliases)
		{
			return ColumnProjector.ProjectColumns(expression, newAlias, existingAliases);
		}

		protected override Expression VisitConstant(ConstantExpression c)
		{
			if (c.Value is IQueryable queryable && queryable.Provider == _provider)
			{
				Type elementType = queryable.ElementType;
				AliasedExpression source;
				if (typeof(Table<>).MakeGenericType(elementType) == queryable.GetType())
				{
					var nameProperty = queryable.GetType().GetRuntimeProperty("Name");
					source = new TableExpression(GetNewAlias(), (string) nameProperty.GetValue(queryable));
				}
				else if (typeof(View<>).MakeGenericType(elementType) == queryable.GetType())
				{
					var nameProperty = queryable.GetType().GetRuntimeProperty("Name");
					source = new ViewExpression(GetNewAlias(), (string) nameProperty.GetValue(queryable));
				}
				else
				{
					return Visit(queryable.Expression);
				}

				string selectAlias = GetNewAlias();

				List<MemberBinding> bindings = new List<MemberBinding>();
				List<ColumnDeclaration> columns = new List<ColumnDeclaration>();

				foreach (PropertyInfo pi in elementType.GetRuntimeProperties().Where(pi => !pi.IsDefined(typeof(IgnoreAttribute))))
				{
					string columnName = Orm.GetColumnName(pi);
					Type columnType = pi.PropertyType;

					bindings.Add(Expression.Bind(pi, new ColumnExpression(columnType, selectAlias, columnName)));
					columns.Add(new ColumnDeclaration(columnName, new ColumnExpression(columnType, source.Alias, columnName)));
				}

				return new ProjectionExpression(
					new SelectExpression(selectAlias, columns, source),
					Expression.MemberInit(Expression.New(elementType), bindings)
				);
			}
			return c;
		}

		protected override Expression VisitParameter(ParameterExpression p)
		{
			return _map.TryGetValue(p, out Expression e) ? e : p;
		}

		protected override Expression VisitMember(MemberExpression m)
		{
			if (m.Member.DeclaringType == typeof(string))
			{
				switch (m.Member.Name)
				{
					case nameof(string.Length):
						return new FunctionExpression(typeof(int), "LENGTH", Visit(m.Expression));
				}
			}
			else if (m.Member.DeclaringType == typeof(DateTime) || m.Member.DeclaringType == typeof(DateTimeOffset))
			{
				switch (m.Member.Name)
				{
					case nameof(DateTime.Year):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%Y"), Visit(m.Expression));
					case nameof(DateTime.Month):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%m"), Visit(m.Expression));
					case nameof(DateTime.Day):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%d"), Visit(m.Expression));
					case nameof(DateTime.Hour):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%H"), Visit(m.Expression));
					case nameof(DateTime.Minute):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%M"), Visit(m.Expression));
					case nameof(DateTime.Second):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%S"), Visit(m.Expression));
					case nameof(DateTime.Millisecond):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Expression.Multiply(
								Expression.Subtract(
									new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%f"), Visit(m.Expression)),
									new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%S"), Visit(m.Expression))
								),
								Expression.Constant(1000)
							));
					case nameof(DateTime.DayOfYear):
						return new FunctionExpression(typeof(int), "STRFTIME", Expression.Constant("%j"), Visit(m.Expression));
					case nameof(DateTime.DayOfWeek):
						return new FunctionExpression(typeof(DayOfWeek), "STRFTIME", Expression.Constant("%w"), Visit(m.Expression));
					case nameof(DateTime.Date):
						return new FunctionExpression(typeof(DateTime), "STRFTIME",
							Expression.Constant(Orm.DateTimeSqlFormat),
							Visit(m.Expression),
							Expression.Constant("start of day")
						);
					case nameof(DateTime.TimeOfDay):
						return Visit(Expression.Call(
							typeof(TimeSpan), nameof(TimeSpan.FromMilliseconds), null,
							Expression.Call(
								typeof(Math), nameof(Math.Round), null,
								Expression.Multiply(
									Expression.Subtract(
										new FunctionExpression(typeof(double), "JULIANDAY", Visit(m.Expression)),
										new FunctionExpression(
											typeof(double), "JULIANDAY", Visit(m.Expression), Expression.Constant("start of day")
										)
									),
									Expression.Constant(24 * 60 * 60 * 1000.0)
								)
							)
						));
					case nameof(DateTime.Today):
						return new FunctionExpression(typeof(DateTime), "DATE", Expression.Constant("now"));
					case nameof(DateTime.Now):
						return new FunctionExpression(
							typeof(DateTime), "STRFTIME",
							Expression.Constant(Orm.DateTimeFormat), Expression.Constant("now")
						);
				}
			}
			else if (m.Member.DeclaringType == typeof(TimeSpan))
			{
				switch (m.Member.Name)
				{
					case nameof(TimeSpan.TotalDays):
						return Expression.Divide(
							Expression.Convert(Visit(Expression.Property(m.Expression, nameof(TimeSpan.Ticks))), typeof(double)),
							Expression.Constant((double)TimeSpan.TicksPerDay)
						);
					case nameof(TimeSpan.TotalHours):
						return Expression.Divide(
							Expression.Convert(Visit(Expression.Property(m.Expression, nameof(TimeSpan.Ticks))), typeof(double)),
							Expression.Constant((double)TimeSpan.TicksPerHour)
						);
					case nameof(TimeSpan.TotalMinutes):
						return Expression.Divide(
							Expression.Convert(Visit(Expression.Property(m.Expression, nameof(TimeSpan.Ticks))), typeof(double)),
							Expression.Constant((double)TimeSpan.TicksPerMinute)
						);
					case nameof(TimeSpan.TotalSeconds):
						return Expression.Divide(
							Expression.Convert(Visit(Expression.Property(m.Expression, nameof(TimeSpan.Ticks))), typeof(double)),
							Expression.Constant((double)TimeSpan.TicksPerSecond)
						);
					case nameof(TimeSpan.TotalMilliseconds):
						return Expression.Divide(
							Expression.Convert(Visit(Expression.Property(m.Expression, nameof(TimeSpan.Ticks))), typeof(double)),
							Expression.Constant((double)TimeSpan.TicksPerMillisecond)
						);
					case nameof(TimeSpan.Ticks):
						return new TypeChangeExpression(typeof(long), Visit(m.Expression));
					case nameof(TimeSpan.Days):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Visit(Expression.Property(m.Expression, nameof(TimeSpan.TotalDays)))
						);
					case nameof(TimeSpan.Hours):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Expression.Modulo(
								Visit(Expression.Property(m.Expression, nameof(TimeSpan.TotalHours))),
								Expression.Constant(24D)
							)
						);
					case nameof(TimeSpan.Minutes):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Expression.Modulo(
								Visit(Expression.Property(m.Expression, nameof(TimeSpan.TotalMinutes))),
								Expression.Constant(60D)
							)
						);
					case nameof(TimeSpan.Seconds):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Expression.Modulo(
								Visit(Expression.Property(m.Expression, nameof(TimeSpan.TotalSeconds))),
								Expression.Constant(60D)
							)
						);
					case nameof(TimeSpan.Milliseconds):
						return new FunctionExpression(
							typeof(int), "ROUND",
							Expression.Modulo(
								Visit(Expression.Property(m.Expression, nameof(TimeSpan.TotalMilliseconds))),
								Expression.Constant(1000D)
							)
						);
				}
			}

			bool MembersMatch(MemberInfo a, MemberInfo b)
			{
				if (Equals(a, b)) return true;

				if (a is MethodInfo && b is PropertyInfo propB)
				{
					return Equals(a, propB.GetMethod);
				}
				if (a is PropertyInfo propA && b is MethodInfo)
				{
					return Equals(propA.GetMethod, b);
				}
				return false;
			}

			Expression source = Visit(m.Expression);
			switch (source.NodeType)
			{
				case ExpressionType.MemberInit:
					MemberInitExpression min = (MemberInitExpression)source;
					var assignment = min.Bindings.OfType<MemberAssignment>()
						.FirstOrDefault(ma => MembersMatch(ma.Member, m.Member));
					if (assignment != null) return assignment.Expression;
					break;
				case ExpressionType.New:
					NewExpression nex = (NewExpression)source;
					if (nex.Members != null)
					{
						for (int i = 0, n = nex.Members.Count; i < n; i++)
						{
							if (MembersMatch(nex.Members[i], m.Member))
							{
								return nex.Arguments[i];
							}
						}
					}
					else if (nex.Type.GetTypeInfo().IsGenericType && nex.Type.GetGenericTypeDefinition() == typeof(Grouping<,>))
					{
						if (m.Member.Name == "Key")
						{
							return nex.Arguments[0];
						}
					}
					break;
			}
			return source == m.Expression ? m : Expression.MakeMemberAccess(source, m.Member);
		}

		protected override Expression VisitMethodCall(MethodCallExpression call)
		{
			if (call.Method.DeclaringType == typeof(Queryable) || call.Method.DeclaringType == typeof(Enumerable))
			{
				MethodInfo mi = call.Method;
				switch (mi.Name)
				{
					case nameof(Queryable.Where):
						return BindWhere(call.Arguments[0], GetLambda(call.Arguments[1]));
					case nameof(Queryable.Select):
						return BindSelect(call.Arguments[0], GetLambda(call.Arguments[1]));
					case nameof(Queryable.SelectMany):
						return BindSelectMany(
							call.Arguments[0], GetLambda(call.Arguments[1]), GetLambda(call.Arguments[2])
						);
					case nameof(Queryable.Join):
						return BindJoin(
							call.Arguments[0], call.Arguments[1],
							GetLambda(call.Arguments[2]),
							GetLambda(call.Arguments[3]),
							GetLambda(call.Arguments[4])
						);
					case nameof(Queryable.GroupJoin):
						return BindGroupJoin(
							call.Arguments[0], call.Arguments[1],
							GetLambda(call.Arguments[2]),
							GetLambda(call.Arguments[3]),
							GetLambda(call.Arguments[4])
						);
					case nameof(Queryable.OrderBy):
						return BindOrderBy(call.Arguments[0], GetLambda(call.Arguments[1]), OrderType.Ascending);
					case nameof(Queryable.OrderByDescending):
						return BindOrderBy(call.Arguments[0], GetLambda(call.Arguments[1]), OrderType.Descending);
					case nameof(Queryable.ThenBy):
						return BindThenBy(call.Arguments[0], GetLambda(call.Arguments[1]), OrderType.Ascending);
					case nameof(Queryable.ThenByDescending):
						return BindThenBy(call.Arguments[0], GetLambda(call.Arguments[1]), OrderType.Descending);
					case nameof(Queryable.GroupBy):
						switch (call.Arguments.Count)
						{
							case 2:
								return BindGroupBy(call.Arguments[0], GetLambda(call.Arguments[1]), null, null);
							case 3:
								LambdaExpression lambda1 = GetLambda(call.Arguments[1]);
								LambdaExpression lambda2 = GetLambda(call.Arguments[2]);
								switch (lambda2.Parameters.Count)
								{
									case 1: // second lambda is element selector
										return BindGroupBy(call.Arguments[0], lambda1, lambda2, null);
									case 2: // second lambda is result selector
										return BindGroupBy(call.Arguments[0], lambda1, null, lambda2);
								}
								break;
							case 4:
								return BindGroupBy(
									call.Arguments[0],
									GetLambda(call.Arguments[1]),
									GetLambda(call.Arguments[2]),
									GetLambda(call.Arguments[3])
								);
						}
						break;
					case nameof(Queryable.Count):
					case nameof(Queryable.LongCount):
					case nameof(Queryable.Min):
					case nameof(Queryable.Max):
					case nameof(Queryable.Sum):
					case nameof(Queryable.Average):
						switch (call.Arguments.Count)
						{
							case 1:
								return BindAggregate(call.Arguments[0], mi, null, call == _root);
							case 2:
								return BindAggregate(call.Arguments[0], mi, GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.Distinct) when call.Arguments.Count == 1:
						return BindDistinct(call.Arguments[0]);
					case nameof(Queryable.Skip) when call.Arguments.Count == 2:
						return BindSkip(call.Arguments[0], call.Arguments[1]);
					case nameof(Queryable.Take) when call.Arguments.Count == 2:
						return BindTake(call.Arguments[0], call.Arguments[1]);
					case nameof(Queryable.ElementAt):
						return BindElementAt(call.Arguments[0], call.Arguments[1], call == _root);
					case nameof(Queryable.ElementAtOrDefault):
						return BindElementAtOrDefault(call.Arguments[0], call.Arguments[1], call == _root);
					case nameof(Queryable.First):
						switch (call.Arguments.Count)
						{
							case 1: return BindFirst(call.Arguments[0], null, call == _root);
							case 2: return BindFirst(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.FirstOrDefault):
						switch (call.Arguments.Count)
						{
							case 1: return BindFirstOrDefault(call.Arguments[0], null, call == _root);
							case 2: return BindFirstOrDefault(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.Single):
						switch (call.Arguments.Count)
						{
							case 1: return BindSingle(call.Arguments[0], null, call == _root);
							case 2: return BindSingle(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.SingleOrDefault):
						switch (call.Arguments.Count)
						{
							case 1: return BindSingleOrDefault(call.Arguments[0], null, call == _root);
							case 2: return BindSingleOrDefault(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.Any):
						switch (call.Arguments.Count)
						{
							case 1: return BindAny(call.Arguments[0], null, call == _root);
							case 2: return BindAny(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
						}
						break;
					case nameof(Queryable.All) when call.Arguments.Count == 2:
						return BindAll(call.Arguments[0], GetLambda(call.Arguments[1]), call == _root);
					case nameof(Queryable.Contains):
						return BindContains(call.Arguments[0], call.Arguments[1], call == _root);
					case nameof(Queryable.Cast):
						return BindCast(call.Arguments[0], call.Method.GetGenericArguments()[0]);
				}
				throw new NotSupportedException($"The method ‘{mi.Name}’ is not supported");
			}
			else if (call.Method.DeclaringType == typeof(string))
			{
				switch (call.Method.Name)
				{
//		            case nameof(string.Equals):
//			            _sb.Append("(");
//			            Visit(call.Object);
//			            _sb.Append(" == (");
//			            Visit(call.Arguments[0]);
//			            _sb.Append("))");
//			            return call;
		            case nameof(string.Contains):
			            return new FunctionExpression(
				            typeof(bool), "GLOB",
				            Visit(Expression.Call(
								((Func<string, string, string, string>)string.Concat).GetMethodInfo(),
								Expression.Constant("*"),
					            call.Arguments[0],
					            Expression.Constant("*")
							)),
				            Visit(call.Object)
			            );
					case nameof(string.StartsWith):
			            return new FunctionExpression(
				            typeof(bool), "GLOB",
				            Visit(Expression.Call(
					            ((Func<string, string, string>)string.Concat).GetMethodInfo(),
					            call.Arguments[0],
					            Expression.Constant("*")
				            )),
				            Visit(call.Object)
			            );
					case nameof(string.EndsWith):
			            return new FunctionExpression(
				            typeof(bool), "GLOB",
				            Visit(Expression.Call(
					            ((Func<string, string, string>)string.Concat).GetMethodInfo(),
								Expression.Constant("*"),
					            call.Arguments[0]
				            )),
				            Visit(call.Object)
			            );
					case nameof(string.ToLower):
						return new FunctionExpression(typeof(string), "LOWER", Visit(call.Object));
					case nameof(string.ToUpper):
						return new FunctionExpression(typeof(string), "UPPER", Visit(call.Object));
					case nameof(string.Replace):
						return new FunctionExpression(
							typeof(string), "REPLACE",
							Visit(call.Object), Visit(call.Arguments[0]), Visit(call.Arguments[1])
						);
		            case nameof(string.IsNullOrEmpty):
			            return Expression.OrElse(
							Expression.Equal(Visit(call.Arguments[0]), Expression.Constant(null, typeof(string))),
							Expression.Equal(Visit(call.Arguments[0]), Expression.Constant(""))
						);
					case nameof(string.Substring):
						return new FunctionExpression(
							typeof(string), "SUBSTR",
							Visit(call.Object),
							Expression.Add(Visit(call.Arguments[0]), Expression.Constant(1)),
							call.Arguments.Count == 2 ? Visit(call.Arguments[1]) : Expression.Constant(8000)
						);
					case nameof(string.Remove):
						if (call.Arguments.Count == 1)
						{
							return new FunctionExpression(
								typeof(string), "SUBSTR",
								Visit(call.Object), Expression.Constant(1), Visit(call.Arguments[0])
							);
						}
						else
						{
							return Expression.Call(
								((Func<string, string, string>)string.Concat).GetMethodInfo(),
								new FunctionExpression(
									typeof(string), "SUBSTR",
									Visit(call.Object), Expression.Constant(1), Visit(call.Arguments[0])
								),
								new FunctionExpression(
									typeof(string), "SUBSTR",
									Visit(call.Object), Expression.Add(Visit(call.Arguments[0]), Visit(call.Arguments[1]))
								)
							);
						}
					case nameof(string.Trim):
						return new FunctionExpression(typeof(string), "TRIM", Visit(call.Object));
					case nameof(string.TrimStart):
						return new FunctionExpression(typeof(string), "LTRIM", Visit(call.Object));
					case nameof(string.TrimEnd):
						return new FunctionExpression(typeof(string), "RTRIM", Visit(call.Object));
				}
			}
			else if (call.Method.DeclaringType == typeof(DateTime))
			{
				Expression ApplyModifier(Expression modifier, string type)
				{
					return new FunctionExpression(
						typeof(DateTime),
						"STRFTIME", Expression.Constant(Orm.DateTimeSqlFormat), Visit(call.Object), Expression.Call(
							((Func<string, string, string>)string.Concat).GetMethodInfo(),
							Expression.Call(
								Visit(modifier),
								typeof(object).GetRuntimeMethod(nameof(ToString), Array.Empty<Type>())
							),
							Expression.Constant(" " + type)
						));
				}

				switch (call.Method.Name)
				{
					case nameof(DateTime.AddYears):
						return ApplyModifier(call.Arguments[0], "years");
					case nameof(DateTime.AddMonths):
						return ApplyModifier(call.Arguments[0], "months");
					case nameof(DateTime.AddDays):
						return ApplyModifier(call.Arguments[0], "days");
					case nameof(DateTime.AddHours):
						return ApplyModifier(call.Arguments[0], "hours");
					case nameof(DateTime.AddMinutes):
						return ApplyModifier(call.Arguments[0], "minutes");
					case nameof(DateTime.AddSeconds):
						return ApplyModifier(call.Arguments[0], "seconds");
					case nameof(DateTime.AddMilliseconds):
						return ApplyModifier(Expression.Divide(call.Arguments[0], Expression.Constant(1000.0)), "seconds");
					case nameof(DateTime.AddTicks):
						return ApplyModifier(Expression.Divide(call.Arguments[0], Expression.Constant(TimeSpan.TicksPerSecond)), "seconds");
					case nameof(DateTime.Add):
						return ApplyModifier(Expression.Property(call.Arguments[0], nameof(TimeSpan.TotalSeconds)), "seconds");
					case nameof(DateTime.Subtract):
						if (call.Arguments[1].Type == typeof(DateTime))
						{
							return Visit(Expression.Call(
								typeof(TimeSpan), nameof(TimeSpan.FromDays), null,
								Expression.Subtract(
									new FunctionExpression(typeof(double), "JULIANDAY", call.Arguments[0]),
									new FunctionExpression(typeof(double), "JULIANDAY", call.Arguments[1])
								)
							));
						}
						else if (call.Arguments[1].Type == typeof(TimeSpan))
						{
							return ApplyModifier(Expression.Negate(Expression.Property(call.Arguments[0], nameof(TimeSpan.TotalSeconds))), "seconds");
						}
						break;
				}
			}
			else if (call.Method.DeclaringType == typeof(TimeSpan))
			{
				switch (call.Method.Name)
				{
					case nameof(TimeSpan.FromDays):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Convert(Expression.Multiply(
								Visit(call.Arguments[0]), Expression.Constant((double)TimeSpan.TicksPerDay)
							), typeof(long))
						));
					case nameof(TimeSpan.FromHours):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Convert(Expression.Multiply(
								Visit(call.Arguments[0]), Expression.Constant((double)TimeSpan.TicksPerHour)
							), typeof(long))
						));
					case nameof(TimeSpan.FromMinutes):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Convert(Expression.Multiply(
								Visit(call.Arguments[0]), Expression.Constant((double)TimeSpan.TicksPerMinute)
							), typeof(long))
						));
					case nameof(TimeSpan.FromSeconds):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Convert(Expression.Multiply(
								Visit(call.Arguments[0]), Expression.Constant((double)TimeSpan.TicksPerSecond)
							), typeof(long))
						));
					case nameof(TimeSpan.FromMilliseconds):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Convert(Expression.Multiply(
								Visit(call.Arguments[0]), Expression.Constant((double)TimeSpan.TicksPerMillisecond)
							), typeof(long))
						));
					case nameof(TimeSpan.FromTicks):
						return new FunctionExpression(typeof(TimeSpan), "ROUND", Visit(call.Arguments[0]));
					case nameof(TimeSpan.Add):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Add(
								Expression.Property(call.Object, nameof(TimeSpan.Ticks)),
								Expression.Property(call.Arguments[0], nameof(TimeSpan.Ticks))
							)
						));
					case nameof(TimeSpan.Subtract):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Subtract(
								Expression.Property(call.Object, nameof(TimeSpan.Ticks)),
								Expression.Property(call.Arguments[0], nameof(TimeSpan.Ticks))
							)
						));
					case nameof(TimeSpan.Negate):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Negate(
								Expression.Property(call.Object, nameof(TimeSpan.Ticks))
							)
						));
					case nameof(TimeSpan.Duration):
						return Visit(Expression.Call(typeof(TimeSpan), nameof(TimeSpan.FromTicks), null,
							Expression.Call(
								typeof(Math), nameof(Math.Abs), null,
								Expression.Property(call.Object, nameof(TimeSpan.Ticks))
							)
						));
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
						return new FunctionExpression(
							typeof(double),
							call.Method.Name.ToUpper(), Visit(call.Arguments[0]));
					case nameof(Math.Atan2):
						return new FunctionExpression(
							typeof(double),
							"ATN2", Visit(call.Arguments[0]), Visit(call.Arguments[1]));
					case nameof(Math.Log):
						if (call.Arguments.Count == 1)
						{
							goto case nameof(Math.Log10);
						}
						break;
					case nameof(Math.Pow):
						return new FunctionExpression(
							typeof(double),
							"POWER", Visit(call.Arguments[0]), Visit(call.Arguments[1]));
					case nameof(Math.Round):
						if (call.Arguments.Count == 1)
						{
							return new FunctionExpression(
								typeof(double),
								"ROUND", Visit(call.Arguments[0]), Expression.Constant(0));
						}
						else if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(int))
						{
							return new FunctionExpression(
								typeof(double),
								"ROUND", Visit(call.Arguments[0]), Visit(call.Arguments[1]));
						}
						break;
				}
			}
			return base.VisitMethodCall(call);
		}

		private Expression BindSelect(Expression source, LambdaExpression selector)
		{
			ProjectionExpression projection = VisitSequence(source);
			_map[selector.Parameters[0]] = projection.Projector;

			Expression expression = Visit(selector.Body);
			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(expression, alias, GetExistingAlias(projection.Source));
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source),
				pc.Projector
			);
		}

		private Expression BindWhere(Expression source, LambdaExpression predicate)
		{
			ProjectionExpression projection = VisitSequence(source);
			_map[predicate.Parameters[0]] = projection.Projector;

			string alias = GetNewAlias();
			Expression where = Visit(predicate.Body);
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, GetExistingAlias(projection.Source));
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, where),
				pc.Projector
			);
		}

		private Expression BindJoin(Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector)
		{
			ProjectionExpression outerProjection = VisitSequence(outerSource);
			ProjectionExpression innerProjection = VisitSequence(innerSource);

			_map[outerKey.Parameters[0]] = outerProjection.Projector;
			Expression outerKeyExpr = Visit(outerKey.Body);

			_map[innerKey.Parameters[0]] = innerProjection.Projector;
			Expression innerKeyExpr = Visit(innerKey.Body);

			_map[resultSelector.Parameters[0]] = outerProjection.Projector;
			_map[resultSelector.Parameters[1]] = innerProjection.Projector;
			Expression resultExpr = Visit(resultSelector.Body);

			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(resultExpr, alias, outerProjection.Source.Alias, innerProjection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(
					alias,
					pc.Columns,
					new JoinExpression(JoinType.InnerJoin, outerProjection.Source, innerProjection.Source, Expression.Equal(outerKeyExpr, innerKeyExpr))
				),
				pc.Projector
			);
		}

		private Expression BindSelectMany(Expression source, LambdaExpression collectionSelector, LambdaExpression resultSelector)
		{
			ProjectionExpression projection = VisitSequence(source);
			_map[collectionSelector.Parameters[0]] = projection.Projector;

			Expression collection = collectionSelector.Body;

			// check for DefaultIfEmpty
			bool defaultIfEmpty = false;
			if (collection is MethodCallExpression mcs &&
				mcs.Method.Name == nameof(Queryable.DefaultIfEmpty) && mcs.Arguments.Count == 1 &&
				(mcs.Method.DeclaringType == typeof(Queryable) || mcs.Method.DeclaringType == typeof(Enumerable)))
			{
				collection = mcs.Arguments[0];
				defaultIfEmpty = true;
			}

			ProjectionExpression collectionProjection = VisitSequence(collection);

			var alias = GetNewAlias();
			ProjectedColumns pc;
			if (resultSelector == null)
			{
				pc = ProjectColumns(collectionProjection.Projector, alias, projection.Source.Alias, collectionProjection.Source.Alias);
			}
			else
			{
				_map[resultSelector.Parameters[0]] = projection.Projector;
				_map[resultSelector.Parameters[1]] = collectionProjection.Projector;
				Expression result = Visit(resultSelector.Body);
				pc = ProjectColumns(result, alias, projection.Source.Alias, collectionProjection.Source.Alias);
			}
			JoinType joinType = defaultIfEmpty ? JoinType.OuterJoin : JoinType.CrossJoin;

			return new ProjectionExpression(
				new SelectExpression(
					alias,
					pc.Columns,
					new JoinExpression(joinType, projection.Source, collectionProjection.Source, null)
				),
				pc.Projector
			);
		}

		private Expression BindGroupJoin(Expression outerSource, Expression innerSource, LambdaExpression outerKey, LambdaExpression innerKey, LambdaExpression resultSelector)
		{
			ProjectionExpression outerProjection = VisitSequence(outerSource);
			ProjectionExpression innerProjection = VisitSequence(innerSource);

			_map[outerKey.Parameters[0]] = outerProjection.Projector;
			Expression outerKeyExpr = Visit(outerKey.Body);

			_map[innerKey.Parameters[0]] = innerProjection.Projector;
			Expression innerKeyExpr = Visit(innerKey.Body);

			LambdaExpression predicate = Expression.Lambda(Expression.Equal(innerKey.Body, outerKey.Body), innerKey.Parameters[0]);
			Expression subquery = Expression.Call(typeof(Queryable), nameof(Queryable.Where), new[] { innerKey.Parameters[0].Type }, innerSource, predicate);

			_map[resultSelector.Parameters[0]] = outerProjection.Projector;
			_map[resultSelector.Parameters[1]] = Visit(subquery);
			Expression resultExpr = Visit(resultSelector.Body);

			JoinExpression join = new JoinExpression(JoinType.InnerJoin, outerProjection.Source, innerProjection.Source, Expression.Equal(outerKeyExpr, innerKeyExpr));

			var groupedColumns = ProjectColumns(outerKeyExpr, outerProjection.Source.Alias, outerProjection.Source.Alias);
			IEnumerable<Expression> groupExprs = groupedColumns.Columns.Select(c => c.Expression);

			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(resultExpr, alias, outerProjection.Source.Alias, innerProjection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, join, groupBy: groupExprs),
				pc.Projector
			);
		}

		private Expression BindGroupBy(Expression source, LambdaExpression keySelector, LambdaExpression elementSelector, LambdaExpression resultSelector)
		{
			ProjectionExpression projection = VisitSequence(source);

			_map[keySelector.Parameters[0]] = projection.Projector;
			Expression keyExpr = Visit(keySelector.Body);

			// Use ProjectColumns to get group-by expressions from key expression
			ProjectedColumns keyProjection = ProjectColumns(keyExpr, projection.Source.Alias, projection.Source.Alias);
			IList<Expression> groupExprs = keyProjection.Columns.Select(c => c.Expression).ToList();

			// make duplicate of source query as basis of element subquery by visiting the source again
			ProjectionExpression subqueryBasis = VisitSequence(source);

			// recompute key columns for group expressions relative to subquery (need these for doing the correlation predicate)
			_map[keySelector.Parameters[0]] = subqueryBasis.Projector;
			Expression subqueryKey = Visit(keySelector.Body);

			// use same projection trick to get group-by expressions based on subquery
			ProjectedColumns subqueryKeyProjection = ProjectColumns(subqueryKey, subqueryBasis.Source.Alias, subqueryBasis.Source.Alias);
			Expression subqueryCorrelation = subqueryKeyProjection.Columns.Select(c => c.Expression).Zip(groupExprs, Expression.Equal).Aggregate(Expression.AndAlso);

			// compute element based on duplicated subquery
			Expression subqueryElemExpr = subqueryBasis.Projector;
			if (elementSelector != null)
			{
				_map[elementSelector.Parameters[0]] = subqueryBasis.Projector;
				subqueryElemExpr = Visit(elementSelector.Body);
			}

			// build subquery that projects the desired element
			string elementAlias = GetNewAlias();
			ProjectedColumns elementProjection = ProjectColumns(subqueryElemExpr, elementAlias, subqueryBasis.Source.Alias);
			ProjectionExpression elementSubquery = new ProjectionExpression(
				new SelectExpression(
					elementAlias,
					elementProjection.Columns,
					subqueryBasis.Source,
					subqueryCorrelation
				),
				elementProjection.Projector
			);

			string alias = GetNewAlias();

			Expression resultExpr;
			if (resultSelector != null) // compute result expression based on key & element-subquery
			{
				_map[resultSelector.Parameters[0]] = keyProjection.Projector;
				_map[resultSelector.Parameters[1]] = elementSubquery;
				resultExpr = Visit(resultSelector.Body);
			}
			else // result must be IGrouping<K,E>
			{
				Type groupingType = typeof(Grouping<,>).MakeGenericType(keyExpr.Type, subqueryElemExpr.Type);
				resultExpr = Expression.New(groupingType.GetTypeInfo().DeclaredConstructors.First(), keyExpr, elementSubquery);
			}

			ProjectedColumns pc = ProjectColumns(resultExpr, alias, projection.Source.Alias);

			return new ProjectionExpression(
				new SelectExpression(
					alias, pc.Columns, projection.Source, groupBy: groupExprs
				),
				pc.Projector
			);
		}

		private Expression BindOrderBy(Expression source, LambdaExpression orderSelector, OrderType orderType)
		{
			List<OrderExpression> myThenBys = _thenBys;
			_thenBys = null;
			ProjectionExpression projection = VisitSequence(source);

			_map[orderSelector.Parameters[0]] = projection.Projector;
			List<OrderExpression> orderings = new List<OrderExpression> { new OrderExpression(orderType, Visit(orderSelector.Body)) };
			if (myThenBys != null)
			{
				for (int i = myThenBys.Count - 1; i >= 0; i--)
				{
					OrderExpression tb = myThenBys[i];
					LambdaExpression lambda = (LambdaExpression)tb.Expression;
					_map[lambda.Parameters[0]] = projection.Projector;
					orderings.Add(new OrderExpression(tb.OrderType, Visit(lambda.Body)));
				}
			}

			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(
					alias,
					pc.Columns,
					projection.Source,
					orderBy: orderings.AsReadOnly()
				),
				pc.Projector
			);
		}

		private Expression BindThenBy(Expression source, LambdaExpression orderSelector, OrderType orderType)
		{
			if (_thenBys == null)
			{
				_thenBys = new List<OrderExpression>();
			}
			_thenBys.Add(new OrderExpression(orderType, orderSelector));
			return Visit(source);
		}

		private Expression BindAggregate(Expression source, MethodInfo method, LambdaExpression argument, bool isRoot)
		{
			bool HasPredicateArg(AggregateType aggregateType)
			{
				return aggregateType == AggregateType.Count;
			}
			AggregateType GetAggregateType(string methodName)
			{
				switch (methodName)
				{
					case nameof(Queryable.Min): return AggregateType.Min;
					case nameof(Queryable.Max): return AggregateType.Max;
					case nameof(Queryable.Sum): return AggregateType.Sum;
					case nameof(Queryable.Count): return AggregateType.Count;
					case nameof(Queryable.LongCount): return AggregateType.Count;
					case nameof(Queryable.Average): return AggregateType.Average;
					default: throw new Exception($"Unknown aggregate type: {methodName}");
				}
			}

			Type returnType = method.ReturnType;
			AggregateType aggType = GetAggregateType(method.Name);
			bool hasPredicateArg = HasPredicateArg(aggType);

			if (argument != null && hasPredicateArg)
			{
				// convert query.Count(predicate) into query.Where(predicate).Count()
				source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), method.GetGenericArguments(), source, argument);
				argument = null;
			}

			ProjectionExpression projection = VisitSequence(source);

			Expression argExpr = null;
			if (argument != null)
			{
				_map[argument.Parameters[0]] = projection.Projector;
				argExpr = Visit(argument.Body);
			}
			else if (!hasPredicateArg)
			{
				argExpr = projection.Projector;
			}

			SelectExpression select = new SelectExpression(
				GetNewAlias(),
				new[] { new ColumnDeclaration("", new AggregateExpression(returnType, aggType, argExpr)) },
				projection.Source
			);
			if (isRoot)
			{
				return new ProjectionExpression(
					select,
					new ColumnExpression(returnType, select.Alias, ""),
					GetAggregator(returnType, nameof(Enumerable.Single))
				);
			}
			return new ScalarExpression(returnType, select);
		}

		private Expression BindDistinct(Expression source)
		{
			ProjectionExpression projection = VisitSequence(source);
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, isDistinct: true),
				pc.Projector
			);
		}

		private Expression BindTake(Expression source, Expression take)
		{
			ProjectionExpression projection = VisitSequence(source);
			take = Visit(take);
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, limit: take),
				pc.Projector
			);
		}

		private Expression BindSkip(Expression source, Expression skip)
		{
			ProjectionExpression projection = VisitSequence(source);
			skip = Visit(skip);
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, offset: skip),
				pc.Projector
			);
		}

		private Expression BindCast(Expression source, Type targetElementType)
		{
			Type GetTrueUnderlyingType(Expression expression)
			{
				while (expression.NodeType == ExpressionType.Convert)
				{
					expression = ((UnaryExpression)expression).Operand;
				}
				return expression.Type;
			}

			ProjectionExpression projection = VisitSequence(source);
			Type elementType = GetTrueUnderlyingType(projection.Projector);
			if (!targetElementType.GetTypeInfo().IsAssignableFrom(elementType.GetTypeInfo()))
			{
				throw new InvalidOperationException($"Cannot cast elements from type '{elementType}' to type '{targetElementType}'");
			}
			return projection;
		}

		private Expression BindSingle(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (predicate != null)
			{
				source = Expression.Call(
					typeof(Enumerable), nameof(Enumerable.Where), new[] { predicate.Parameters[0].Type }, source, predicate
				);
			}

			ProjectionExpression projection = VisitSequence(source);
			Type elementType = projection.Projector.Type;
			if (isRoot)
			{
				return new ProjectionExpression(
					projection.Source,
					projection.Projector,
					GetAggregator(elementType, nameof(Enumerable.Single))
				);
			}
			else if (Orm.IsColumnTypeSupported(elementType))
			{
				return new ScalarExpression(elementType, projection.Source);
			}
			else
			{
				throw new ArgumentException("Cannot be converted to SQL.");
			}
		}

		private Expression BindSingleOrDefault(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (predicate != null)
			{
				source = Expression.Call(
					typeof(Enumerable), nameof(Enumerable.Where), new[] { predicate.Parameters[0].Type }, source, predicate
				);
			}

			ProjectionExpression projection = VisitSequence(source);
			Type elementType = projection.Projector.Type;
			if (isRoot)
			{
				return new ProjectionExpression(
					projection.Source,
					projection.Projector,
					GetAggregator(elementType, nameof(Enumerable.SingleOrDefault))
				);
			}
			else if (Orm.IsColumnTypeSupported(elementType))
			{
				return Expression.Coalesce(
					new ScalarExpression(elementType, projection.Source),
					Expression.Constant(GetDefaultValue(elementType))
				);
			}
			else
			{
				throw new ArgumentException("Cannot be converted to SQL.");
			}
		}

		private Expression BindFirst(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (predicate != null) source = BindWhere(source, predicate);
			return BindSingle(BindTake(source, Expression.Constant(1)), null, isRoot);
		}

		private Expression BindFirstOrDefault(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (predicate != null) source = BindWhere(source, predicate);
			return BindSingleOrDefault(BindTake(source, Expression.Constant(1)), null, isRoot);
		}

		private Expression BindElementAt(Expression source, Expression index, bool isRoot)
		{
			return BindFirst(BindSkip(source, index), null, isRoot);
		}

		private Expression BindElementAtOrDefault(Expression source, Expression index, bool isRoot)
		{
			return BindFirstOrDefault(BindSkip(source, index), null, isRoot);
		}

		private Expression BindAll(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(!isRoot);
				Expression where = ((IEnumerable)constSource.Value).Cast<object>()
					.Select(value => Expression.Invoke(predicate, Expression.Constant(value, predicate.Parameters[0].Type)))
					.Cast<Expression>()
					.Aggregate(Expression.AndAlso);
				return Visit(where);
			}
			else
			{
				predicate = Expression.Lambda(Expression.Not(predicate.Body), predicate.Parameters);
				source = Expression.Call(
					typeof(Enumerable), nameof(Enumerable.Where), new[] { predicate.Parameters[0].Type }, source, predicate
				);
				ProjectionExpression projection = VisitSequence(source);
				Expression result = Expression.Not(new ExistsExpression(projection.Source));
				if (isRoot)
				{
					string alias = GetNewAlias();
					return new ProjectionExpression(
						new SelectExpression(
							alias,
							new[] { new ColumnDeclaration("value", result) },
							null
						),
						new ColumnExpression(typeof(bool), alias, "value"),
						GetAggregator<bool>(r => r.Single())
					);
				}
				return result;
			}
		}

		private Expression BindAny(Expression source, LambdaExpression predicate, bool isRoot)
		{
			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(!isRoot);
				Expression where = ((IEnumerable)constSource.Value).Cast<object>()
					.Select(value => Expression.Invoke(predicate, Expression.Constant(value, predicate.Parameters[0].Type)))
					.Cast<Expression>()
					.Aggregate(Expression.OrElse);
				return Visit(where);
			}
			else
			{
				if (predicate != null)
				{
					source = Expression.Call(
						typeof(Enumerable), nameof(Enumerable.Where), new[] { predicate.Parameters[0].Type }, source, predicate
					);
				}
				ProjectionExpression projection = VisitSequence(source);
				Expression result = new ExistsExpression(projection.Source);
				if (isRoot)
				{
					string alias = GetNewAlias();
					return new ProjectionExpression(
						new SelectExpression(
							alias,
							new[] { new ColumnDeclaration("value", result) },
							null
						),
						new ColumnExpression(typeof(bool), alias, "value"),
						GetAggregator<bool>(r => r.Single())
					);
				}
				return result;
			}
		}

		private Expression BindContains(Expression source, Expression match, bool isRoot)
		{
			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(!isRoot);
				List<Expression> values = ((IEnumerable)constSource.Value).Cast<object>()
					.Select(value => Expression.Constant(Convert.ChangeType(value, match.Type), match.Type))
					.Cast<Expression>()
					.ToList();
				return new InExpression(Visit(match), values);
			}
			else if (isRoot)
			{
				var p = Expression.Parameter(TypeSystem.GetElementType(source.Type), "x");
				var predicate = Expression.Lambda(Expression.Equal(p, match), p);
				var exp = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { p.Type }, source, predicate);
				_root = exp;
				return Visit(exp);
			}
			else
			{
				ProjectionExpression projection = VisitSequence(source);
				return new InExpression(Visit(match), projection.Source);
			}
		}

		private ProjectionExpression VisitSequence(Expression sourceExpr)
		{
			Expression expr = Visit(sourceExpr);
			switch (expr.NodeType)
			{
				case (ExpressionType)DbExpressionType.Projection:
					return (ProjectionExpression)expr;
				case ExpressionType.New:
					NewExpression nex = (NewExpression)expr;
					if (expr.Type.GetTypeInfo().IsGenericType && expr.Type.GetGenericTypeDefinition() == typeof(Grouping<,>))
					{
						return (ProjectionExpression)nex.Arguments[1];
					}
					goto default;
				default:
					throw new Exception($"The expression of type '{expr.Type}' is not a sequence");
			}
		}

		private bool IsQuery(Expression expression)
		{
			Type elementType = TypeSystem.GetElementType(expression.Type);
			return elementType != null && typeof(IQueryable<>).MakeGenericType(elementType).GetTypeInfo()
					   .IsAssignableFrom(expression.Type.GetTypeInfo());
		}

		private static LambdaExpression GetAggregator(Type elementType, string methodName)
		{
			ParameterExpression p = Expression.Parameter(typeof(IEnumerable<>).MakeGenericType(elementType), "p");
			return Expression.Lambda(
				Expression.Call(typeof(Enumerable), methodName, new[] { elementType }, p),
				p
			);
		}

		private static LambdaExpression GetAggregator<T>(Expression<Func<IEnumerable<T>, T>> aggregator)
		{
			return aggregator;
		}

		private static object GetDefaultValue(Type type)
		{
			return TypeSystem.IsNullAssignable(type) ? null : Activator.CreateInstance(type);
		}
	}
}
