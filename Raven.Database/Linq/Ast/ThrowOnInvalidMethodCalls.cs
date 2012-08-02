using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;
using System.Linq;

namespace Raven.Database.Linq.Ast
{
	public class ThrowOnInvalidMethodCalls : AbstractAstVisitor
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

		public List<ForbiddenMethod> Members = new List<ForbiddenMethod>
		{
			new ForbiddenMethod(
				names: new []{"Now", "UtcNow"},
				typeAliases: new[]{"DateTime", "System.DateTime", "DateTimeOffset", "System.DateTimeOffset"},
				error: @"Cannot use {0} during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using {0} invalidate that premise, and is not allowed"),
		};

		public override object VisitQueryExpressionOrderClause(QueryExpressionOrderClause queryExpressionOrderClause, object data)
		{
			var text = QueryParsingUtils.ToText(queryExpressionOrderClause);
			throw new InvalidOperationException(
				@"OrderBy calls are not valid during map or reduce phase, but the following was found:
" + text + @"
OrderBy calls modify the indexing output, but doesn't actually impact the order of results returned from the database.
You should be calling OrderBy on the QUERY, not on the index, if you want to specify ordering.");
		}

		public override object VisitQueryExpressionLetClause(QueryExpressionLetClause queryExpressionLetClause, object data)
		{
			if (SimplifyLetExpression(queryExpressionLetClause.Expression) is LambdaExpression)
			{
				var text = QueryParsingUtils.ToText(queryExpressionLetClause);
				throw new SecurityException("Let expression cannot contain labmda expressions, but got: " + text);
			}

			return base.VisitQueryExpressionLetClause(queryExpressionLetClause, data);
		}

		public override object VisitLambdaExpression(LambdaExpression lambdaExpression, object data)
		{
			if (lambdaExpression.StatementBody == null || lambdaExpression.StatementBody.IsNull)
				return base.VisitLambdaExpression(lambdaExpression, data);
			var text = QueryParsingUtils.ToText(lambdaExpression);
			throw new SecurityException("Lambda expression can only consist of a single expression, not a statement, but got: " + text);
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

		public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
		{
			foreach (var forbidden in Members.Where(x => x.Names.Contains(memberReferenceExpression.MemberName)))
			{
				var identifierExpression = GetTarget(memberReferenceExpression);
				if(forbidden.TypeAliases.Contains(identifierExpression) == false)
					continue;

				var text = QueryParsingUtils.ToText(memberReferenceExpression);
				throw new InvalidOperationException(string.Format(forbidden.Error, text));
			}

			return base.VisitMemberReferenceExpression(memberReferenceExpression, data);
		}

		private static string GetTarget(MemberReferenceExpression memberReferenceExpression)
		{
			var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
			if(identifierExpression!=null)
				return identifierExpression.Identifier;

			var mre = memberReferenceExpression.TargetObject as MemberReferenceExpression;
			if(mre != null)
				return GetTarget(mre) + "." + mre.MemberName;

			return null;
		}
	}
}