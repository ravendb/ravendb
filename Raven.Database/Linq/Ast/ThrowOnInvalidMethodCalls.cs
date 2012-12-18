using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class ThrowOnInvalidMethodCalls : DepthFirstAstVisitor<object, object>
	{
		public class ForbiddenMethod
		{
			public string[] TypeAliases;
			public string[] Names;
			public string Error;

			public ForbiddenMethod(string[] names, string[] typeAliases, string error)
			{
				TypeAliases = typeAliases;
				Names = names;
				Error = error;
			}
		}

		private readonly string groupByIdentifier;

		public ThrowOnInvalidMethodCalls(string groupByIdentifier)
		{
			this.groupByIdentifier = groupByIdentifier;
		}

		public List<ForbiddenMethod> Members = new List<ForbiddenMethod>
		{
			new ForbiddenMethod(
				names: new[] { "Now", "UtcNow" },
				typeAliases: new[] { "DateTime", "System.DateTime", "DateTimeOffset", "System.DateTimeOffset" },
				error: @"Cannot use {0} during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using {0} invalidate that premise, and is not allowed"),
		};

		public override object VisitQueryOrderClause(QueryOrderClause queryOrderClause, object data)
		{
			var text = QueryParsingUtils.ToText(queryOrderClause);
			throw new InvalidOperationException(
				@"OrderBy calls are not valid during map or reduce phase, but the following was found:
" + text + @"
OrderBy calls modify the indexing output, but doesn't actually impact the order of results returned from the database.
You should be calling OrderBy on the QUERY, not on the index, if you want to specify ordering.");
		}

		public override object VisitQueryLetClause(QueryLetClause queryLetClause, object data)
		{
			if (SimplifyLetExpression(queryLetClause.Expression) is LambdaExpression)
			{
				var text = QueryParsingUtils.ToText(queryLetClause);
				throw new SecurityException("Let expression cannot contain labmda expressions, but got: " + text);
			}

			return base.VisitQueryLetClause(queryLetClause, data);
		}

		public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
		{
			if (!string.IsNullOrEmpty(groupByIdentifier))
			{
				var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;
				if (memberReferenceExpression != null)
				{
					var identifier = memberReferenceExpression.Target as IdentifierExpression;
					if (identifier != null && identifier.Identifier == groupByIdentifier)
					{
						if (memberReferenceExpression.MemberName == "Count")
							throw new InvalidOperationException("Reduce cannot contain Count() methods in grouping.");

						if (memberReferenceExpression.MemberName == "Average")
							throw new InvalidOperationException("Reduce cannot contain Average() methods in grouping.");
					}
				}
			}

			return base.VisitInvocationExpression(invocationExpression, data);
		}

		private Expression SimplifyLetExpression(Expression expression)
		{
			var castExpression = expression as CastExpression;
			if (castExpression != null)
				return SimplifyLetExpression(castExpression.Expression);
			var parenthesizedExpression = expression as ParenthesizedExpression;
			if (parenthesizedExpression != null)
				return SimplifyLetExpression(parenthesizedExpression.Expression);
			return expression;
		}

		public override object VisitLambdaExpression(LambdaExpression lambdaExpression, object data)
		{
			if (lambdaExpression.Body == null || lambdaExpression.Body.IsNull)
				return base.VisitLambdaExpression(lambdaExpression, data);
			if (lambdaExpression.Body is BlockStatement == false)
				return base.VisitLambdaExpression(lambdaExpression, data);

			var text = QueryParsingUtils.ToText(lambdaExpression);
			throw new SecurityException("Lambda expression can only consist of a single expression, not a statement, but got: " + text);
		}

		public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
		{
			foreach (var forbidden in Members.Where(x => x.Names.Contains(memberReferenceExpression.MemberName)))
			{
				var identifierExpression = GetTarget(memberReferenceExpression);
				if (forbidden.TypeAliases.Contains(identifierExpression) == false)
					continue;

				var text = QueryParsingUtils.ToText(memberReferenceExpression);
				throw new InvalidOperationException(string.Format(forbidden.Error, text));
			}

			return base.VisitMemberReferenceExpression(memberReferenceExpression, data);
		}

		public override object VisitSimpleType(SimpleType simpleType, object data)
		{
			if (simpleType.Identifier.Contains("IGrouping"))
			{
				HandleGroupBy(simpleType);
			}

			return base.VisitSimpleType(simpleType, data);
		}

		private static void HandleGroupBy(SimpleType simpleType)
		{
			var initializer = simpleType.Ancestors.OfType<VariableInitializer>().Single();
			var rootExpression = (InvocationExpression) initializer.Initializer;

			var nodes = rootExpression.Children.Where(x => x.NodeType != NodeType.Token).ToList();
			if (nodes.Count < 2)
				return;

			var memberReferences = nodes.OfType<MemberReferenceExpression>().ToList();
			if (!memberReferences.Any())
				return;

			var groupByExpression = memberReferences.FirstOrDefault(ContainsGroupBy);
			if (groupByExpression == null)
				return;

			var indexOfGroupByExpression = nodes.IndexOf(groupByExpression);
			if (indexOfGroupByExpression != 0)
				return;

			var castExpression = nodes[indexOfGroupByExpression + 1] as CastExpression;
			if (castExpression == null)
				return;

			if (castExpression.Descendants.Contains(simpleType) == false)
				return;

			foreach (var ancestor in simpleType.Ancestors)
			{
				if (ancestor == groupByExpression || groupByExpression.Ancestors.Contains(ancestor) || groupByExpression.Descendants.Contains(ancestor))
					continue;

				if (ancestor.Children.OfType<MemberReferenceExpression>().Any(ContainsGroupBy))
					return;
			}

			var grouping = simpleType.NextSibling;

			var lambda = grouping.Children.OfType<LambdaExpression>().First();
			var parameter = lambda.Parameters.First();

			foreach (var invocation in lambda.Descendants.OfType<InvocationExpression>())
			{
				var identifiers = invocation.Descendants.OfType<IdentifierExpression>().Where(x => x.Identifier == parameter.Name);

				foreach (var identifier in identifiers)
				{
					var parent = identifier.Parent as InvocationExpression;
					if (parent == null)
						continue;

					var member = (MemberReferenceExpression) parent.Target;

					if (member.MemberName == "Count")
						throw new InvalidOperationException("Reduce cannot contain Count() methods in grouping.");

					if (member.MemberName == "Average")
						throw new InvalidOperationException("Reduce cannot contain Average() methods in grouping.");
				}
			}
		}

		private static bool ContainsGroupBy(MemberReferenceExpression possibleGroupByExpression)
		{
			if (possibleGroupByExpression == null)
				return false;

			if (possibleGroupByExpression.MemberName == "GroupBy")
				return true;

			var invocation = possibleGroupByExpression.Target as InvocationExpression;
			if (invocation == null)
				return false;

			var member = invocation.Target as MemberReferenceExpression;
			if (member == null)
				return false;

			return ContainsGroupBy(member);
		}

		private static string GetTarget(MemberReferenceExpression memberReferenceExpression)
		{
			var identifierExpression = memberReferenceExpression.Target as IdentifierExpression;
			if (identifierExpression != null)
				return identifierExpression.Identifier;

			var mre = memberReferenceExpression.Target as MemberReferenceExpression;
			if (mre != null)
				return GetTarget(mre) + "." + mre.MemberName;

			return null;
		}
	}
}
