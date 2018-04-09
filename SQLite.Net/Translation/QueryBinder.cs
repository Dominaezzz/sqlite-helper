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
		private readonly Expression _root;
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

		protected override Expression VisitProjection(ProjectionExpression proj)
		{
			if (proj.Source is RawQueryExpression rawQuery && rawQuery.Alias == null)
			{
				string alias = GetNewAlias();
				var pc = ProjectColumns(proj.Projector, alias, rawQuery.Alias);
				return new ProjectionExpression(
					new RawQueryExpression(rawQuery.Type, alias, rawQuery.SQLQuery),
					pc.Projector
				);
			}
			return base.VisitProjection(proj);
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
						return BindWhere(call);
					case nameof(Queryable.Select):
						return BindSelect(call);
					case nameof(Queryable.SelectMany):
						return BindSelectMany(call);
					case nameof(Queryable.Join):
						return BindJoin(call);
					case nameof(Queryable.GroupJoin):
						return BindGroupJoin(call);
					case nameof(Queryable.OrderBy):
					case nameof(Queryable.OrderByDescending):
						return BindOrderBy(call);
					case nameof(Queryable.ThenBy):
					case nameof(Queryable.ThenByDescending):
						return BindThenBy(call);
					case nameof(Queryable.GroupBy):
						return BindGroupBy(call);
					case nameof(Queryable.Count):
					case nameof(Queryable.LongCount):
					case nameof(Queryable.Min):
					case nameof(Queryable.Max):
					case nameof(Queryable.Sum):
					case nameof(Queryable.Average):
						return BindAggregate(call);
					case nameof(Queryable.Distinct) when call.Arguments.Count == 1:
						return BindDistinct(call);
					case nameof(Queryable.Skip):
						return BindSkip(call);
					case nameof(Queryable.Take):
						return BindTake(call);
					case nameof(Queryable.ElementAt):
					case nameof(Queryable.ElementAtOrDefault):
						return BindElementAt(call);
					case nameof(Queryable.First):
					case nameof(Queryable.FirstOrDefault):
						return BindFirst(call);
					case nameof(Queryable.Single):
					case nameof(Queryable.SingleOrDefault):
						return BindSingle(call);
					case nameof(Queryable.Any):
						return BindAny(call);
					case nameof(Queryable.All):
						return BindAll(call);
					case nameof(Queryable.Contains):
						return BindContains(call);
					case nameof(Queryable.Cast):
						return BindCast(call);
				}
				throw new NotSupportedException($"The method ‘{mi.Name}’ is not supported");
			}
			return base.VisitMethodCall(call);
		}

		private Expression BindSelect(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression selector = GetLambda(call.Arguments[1]);

			ProjectionExpression projection = VisitSequence(source);
			_map[selector.Parameters[0]] = projection.Projector;
			Expression select = Visit(selector.Body);
			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(select, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source),
				pc.Projector
			);
		}

		private Expression BindWhere(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression predicate = GetLambda(call.Arguments[1]);

			ProjectionExpression projection = VisitSequence(source);
			
			_map[predicate.Parameters[0]] = projection.Projector;
			Expression where = Visit(predicate.Body);
			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, where),
				pc.Projector
			);
		}

		private Expression BindJoin(MethodCallExpression call)
		{
			Expression outerSource = call.Arguments[0];
			Expression innerSource = call.Arguments[1];
			LambdaExpression outerKey = GetLambda(call.Arguments[2]);
			LambdaExpression innerKey = GetLambda(call.Arguments[3]);
			LambdaExpression resultSelector = GetLambda(call.Arguments[4]);

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

		private Expression BindSelectMany(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression collectionSelector = GetLambda(call.Arguments[1]);
			LambdaExpression resultSelector = call.Arguments.Count == 3 ? GetLambda(call.Arguments[2]) : null;
			
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

		private Expression BindGroupJoin(MethodCallExpression call)
		{
			Expression outerSource = call.Arguments[0];
			Expression innerSource = call.Arguments[1];
			LambdaExpression outerKey = GetLambda(call.Arguments[2]);
			LambdaExpression innerKey = GetLambda(call.Arguments[3]);
			LambdaExpression resultSelector = GetLambda(call.Arguments[4]);

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

		private Expression BindGroupBy(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression keySelector = GetLambda(call.Arguments[1]);
			LambdaExpression elementSelector = null;
			LambdaExpression resultSelector = null;

			switch (call.Arguments.Count)
			{
				case 3:
					LambdaExpression lambda2 = GetLambda(call.Arguments[2]);
					switch (lambda2.Parameters.Count)
					{
						case 1: // second lambda is element selector
							elementSelector = lambda2;
							break;
						case 2: // second lambda is result selector
							resultSelector = lambda2;
							break;
					}
					break;
				case 4:
					elementSelector = GetLambda(call.Arguments[2]);
					resultSelector = GetLambda(call.Arguments[3]);
					break;
			}

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

		private Expression BindOrderBy(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression orderSelector = GetLambda(call.Arguments[1]);
			OrderType orderType = call.Method.Name.EndsWith("Descending") ? OrderType.Descending : OrderType.Ascending;

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

		private Expression BindThenBy(MethodCallExpression call)
		{
			OrderType orderType = call.Method.Name.EndsWith("Descending") ? OrderType.Descending : OrderType.Ascending;
			if (_thenBys == null)
			{
				_thenBys = new List<OrderExpression>();
			}
			_thenBys.Add(new OrderExpression(orderType, GetLambda(call.Arguments[1])));
			return Visit(call.Arguments[0]);
		}

		private Expression BindAggregate(MethodCallExpression call)
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

			Expression source = call.Arguments[0];
			LambdaExpression argument = call.Arguments.Count == 2 ? GetLambda(call.Arguments[1]) : null;

			Type returnType = call.Method.ReturnType;
			AggregateType aggType = GetAggregateType(call.Method.Name);
			bool hasPredicateArg = HasPredicateArg(aggType);

			if (argument != null && hasPredicateArg)
			{
				// convert query.Count(predicate) into query.Where(predicate).Count()
				source = Expression.Call(typeof(Queryable), nameof(Queryable.Where), call.Method.GetGenericArguments(), source, argument);
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
			if (call == _root)
			{
				return new ProjectionExpression(
					select,
					new ColumnExpression(returnType, select.Alias, ""),
					GetAggregator(returnType, nameof(Enumerable.Single))
				);
			}
			return new ScalarExpression(returnType, select);
		}

		private Expression BindDistinct(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			
			ProjectionExpression projection = VisitSequence(source);
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, isDistinct: true),
				pc.Projector
			);
		}

		private Expression BindTake(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			Expression take = call.Arguments[1];

			ProjectionExpression projection = VisitSequence(source);
			Expression visitedTake = Visit(take);
			
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, limit: visitedTake),
				pc.Projector
			);
		}

		private Expression BindSkip(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			Expression skip = call.Arguments[1];

			ProjectionExpression projection = VisitSequence(source);
			Expression visitedSkip = Visit(skip);
			
			var alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
			return new ProjectionExpression(
				new SelectExpression(alias, pc.Columns, projection.Source, offset: visitedSkip),
				pc.Projector
			);
		}

		private Expression BindCast(MethodCallExpression call)
		{
			Type GetTrueUnderlyingType(Expression expression)
			{
				while (expression.NodeType == ExpressionType.Convert)
				{
					expression = ((UnaryExpression)expression).Operand;
				}
				return expression.Type;
			}

			Expression source = call.Arguments[0];
			Type targetElementType = call.Method.GetGenericArguments()[0];

			ProjectionExpression projection = VisitSequence(source);
			Type elementType = GetTrueUnderlyingType(projection.Projector);
			if (!targetElementType.GetTypeInfo().IsAssignableFrom(elementType.GetTypeInfo()))
			{
				throw new InvalidOperationException($"Cannot cast elements from type '{elementType}' to type '{targetElementType}'");
			}
			return projection;
		}

		private Expression BindSingle(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression predicate = call.Arguments.Count == 2 ? GetLambda(call.Arguments[1]) : null;

			ProjectionExpression projection = VisitSequence(source);
			
			if (predicate != null)
			{
				_map[predicate.Parameters[0]] = projection.Projector;
				Expression where = Visit(predicate.Body);
				string alias = GetNewAlias();
				ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);
				return HandleAggregate(
					call, new SelectExpression(alias, pc.Columns, projection.Source, where), pc.Projector
				);
			}
			else
			{
				return HandleAggregate(call, projection.Source, projection.Projector);
			}
		}

		private Expression BindFirst(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression predicate = call.Arguments.Count == 2 ? GetLambda(call.Arguments[1]) : null;

			ProjectionExpression projection = VisitSequence(source);
			
			Expression where = null;
			if (predicate != null)
			{
				_map[predicate.Parameters[0]] = projection.Projector;
				where = Visit(predicate.Body);
			}

			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);

			return HandleAggregate(
				call,
				new SelectExpression(alias, pc.Columns, projection.Source, where, limit: Expression.Constant(1)),
				pc.Projector
			);
		}
		
		private Expression BindElementAt(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			Expression index = call.Arguments[1];

			ProjectionExpression projection = VisitSequence(source);
			
			string alias = GetNewAlias();
			ProjectedColumns pc = ProjectColumns(projection.Projector, alias, projection.Source.Alias);

			return HandleAggregate(
				call,
				new SelectExpression(alias, pc.Columns, projection.Source,
					offset: Visit(index), limit: Expression.Constant(1)),
				pc.Projector);
		}

		private Expression BindAll(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression predicate = GetLambda(call.Arguments[1]);

			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(call != _root);
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
				if (call == _root)
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

		private Expression BindAny(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			LambdaExpression predicate = call.Arguments.Count == 2 ? GetLambda(call.Arguments[1]) : null;
 
			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(call != _root);
				Expression where = ((IEnumerable)constSource.Value).Cast<object>()
					.Select(value => Expression.Invoke(predicate, Expression.Constant(value, predicate.Parameters[0].Type)))
					.Cast<Expression>()
					.Aggregate(Expression.OrElse);
				return Visit(where);
			}
			else
			{
				ProjectionExpression projection = VisitSequence(source);
				Expression result;
				if (predicate != null)
				{
					_map[predicate.Parameters[0]] = projection.Projector;
					Expression where = Visit(predicate.Body);
					var pc = ProjectColumns(projection.Projector, null, projection.Source.Alias);
					result = new ExistsExpression(new SelectExpression(null, pc.Columns, projection.Source, where));
				}
				else
				{
					result = new ExistsExpression(projection.Source);
				}
 
				if (call == _root)
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

		private Expression BindContains(MethodCallExpression call)
		{
			Expression source = call.Arguments[0];
			Expression item = call.Arguments[1];

			if (source is ConstantExpression constSource && !IsQuery(constSource))
			{
				Debug.Assert(call != _root);
				IEnumerable<Expression> values = ((IEnumerable)constSource.Value).Cast<object>()
					.Select(value => Expression.Constant(Convert.ChangeType(value, item.Type), item.Type));
				return new InExpression(Visit(item), values);
			}
			else
			{
				ProjectionExpression projection = VisitSequence(source);
				Expression result = new InExpression(Visit(item), projection.Source);
				if (call == _root)
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

		private Expression HandleAggregate(MethodCallExpression call, QueryExpression query, Expression projector) 
		{ 
			Type elementType = call.Method.ReturnType; 
			bool hasOrDefault = call.Method.Name.EndsWith("OrDefault"); 
			if (call == _root) 
			{ 
				return new ProjectionExpression( 
					query, projector, 
					GetAggregator(elementType, hasOrDefault ? nameof(Enumerable.SingleOrDefault) : nameof(Enumerable.Single)) 
				); 
			} 
			if (Orm.IsColumnTypeSupported(elementType)) 
			{ 
				if (!hasOrDefault) return new ScalarExpression(elementType, query); 
				return Expression.Coalesce(new ScalarExpression(elementType, query), Expression.Default(elementType)); 
			} 
			throw new ArgumentException("Cannot be converted to SQL."); 
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
