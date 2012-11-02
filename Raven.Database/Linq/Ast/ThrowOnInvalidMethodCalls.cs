using System;
using System.Collections.Generic;
using System.Linq;
using System.Security;
using ICSharpCode.NRefactory.CSharp;

namespace Raven.Database.Linq.Ast
{
	[CLSCompliant(false)]
	public class ThrowOnInvalidMethodCalls : DepthFirstAstVisitor<object,object>
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
			if(lambdaExpression.Body is BlockStatement == false)
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

		private static string GetTarget(MemberReferenceExpression memberReferenceExpression)
		{
			var identifierExpression = memberReferenceExpression.Target as IdentifierExpression;
			if(identifierExpression!=null)
				return identifierExpression.Identifier;

			var mre = memberReferenceExpression.Target as MemberReferenceExpression;
			if(mre != null)
				return GetTarget(mre) + "." + mre.MemberName;

			return null;
		}
	}
}