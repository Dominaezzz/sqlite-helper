﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SQLite.Net.Helpers
{
	public static class Evaluator
	{
		/// <summary>
		/// Performs evaluation & replacement of independent sub-trees
		/// </summary>
		/// <param name="expression">The root of the expression tree.</param>
		/// <param name="fnCanBeEvaluated">A function that decides whether a given expression node can be part of the local function.</param>
		/// <returns>A new tree with sub-trees evaluated and replaced.</returns>
		public static Expression PartialEval(Expression expression, Func<Expression, bool> fnCanBeEvaluated)
		{
			return SubtreeEvaluator.Eval(Nominator.Nominate(fnCanBeEvaluated, expression), expression);
		}

		/// <summary>
		/// Performs evaluation & replacement of independent sub-trees
		/// </summary>
		/// <param name="expression">The root of the expression tree.</param>
		/// <returns>A new tree with sub-trees evaluated and replaced.</returns>
		public static Expression PartialEval(Expression expression)
		{
			return PartialEval(expression, CanBeEvaluatedLocally);
		}

		private static bool CanBeEvaluatedLocally(Expression expression)
		{
			return expression.NodeType != ExpressionType.Parameter;
		}

		/// <summary>
		/// Evaluates & replaces sub-trees when first candidate is reached (top-down)
		/// </summary>
		private class SubtreeEvaluator : DbExpressionVisitor
		{
			private readonly HashSet<Expression> _candidates;

			private SubtreeEvaluator(HashSet<Expression> candidates)
			{
				_candidates = candidates;
			}

			internal static Expression Eval(HashSet<Expression> candidates, Expression exp)
			{
				return new SubtreeEvaluator(candidates).Visit(exp);
			}

			public override Expression Visit(Expression exp)
			{
				if (exp == null) return null;

				if (_candidates.Contains(exp)) return Evaluate(exp);
				return base.Visit(exp);
			}

			private static Expression Evaluate(Expression e)
			{
				if (e.NodeType == ExpressionType.Constant) return e;

				Delegate fn = Expression.Lambda(e).Compile();
				return Expression.Constant(fn.DynamicInvoke(), e.Type);
			}
		}

		/// <summary>
		/// Performs bottom-up analysis to determine which nodes can possibly
		/// be part of an evaluated sub-tree.
		/// </summary>
		private class Nominator : DbExpressionVisitor
		{
			private readonly Func<Expression, bool> _fnCanBeEvaluated;
			private readonly HashSet<Expression> _candidates = new HashSet<Expression>();
			private bool _cannotBeEvaluated;

			private Nominator(Func<Expression, bool> fnCanBeEvaluated)
			{
				_fnCanBeEvaluated = fnCanBeEvaluated;
			}

			internal static HashSet<Expression> Nominate(Func<Expression, bool> fnCanBeEvaluated, Expression expression)
			{
				Nominator nominator = new Nominator(fnCanBeEvaluated);
				nominator.Visit(expression);
				return nominator._candidates;
			}

			public override Expression Visit(Expression expression)
			{
				if (expression == null) return null;

				bool saveCannotBeEvaluated = _cannotBeEvaluated;
				_cannotBeEvaluated = false;
				base.Visit(expression);
				if (!_cannotBeEvaluated)
				{
					if (_fnCanBeEvaluated(expression))
					{
						_candidates.Add(expression);
					}
					else
					{
						_cannotBeEvaluated = true;
					}
				}
				_cannotBeEvaluated |= saveCannotBeEvaluated;
				return expression;
			}
		}
	}
}
