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
					source = new TableExpression(GetNewAlias(), (string) nameProperty.GetValue(queryable));
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
			return base.VisitMethodCall(call);
		}

		private Expression BindSelect(Expression source, LambdaExpression selector)
		{
			ProjectionExpression projection = VisitSequence(source);
			_map[selector.Parameters[0]] = projection.Projector;

			Expression expression = Visit(selector.Body);
			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(expression, alias, projection.Source.Alias);
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
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
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

			bool isDistinct = (projection.Source as SelectExpression)?.IsDistinct ?? false;

			SelectExpression select = new SelectExpression(
				GetNewAlias(),
				new[] { new ColumnDeclaration("", new AggregateExpression(returnType, aggType, argExpr, isDistinct)) },
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
