using System;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class ThrowOnInvalidMethodCalls : AbstractAstVisitor
	{
		public override object VisitQueryExpressionOrderClause(QueryExpressionOrderClause queryExpressionOrderClause, object data)
		{
			var text = QueryParsingUtils.ToText(queryExpressionOrderClause);
			throw new InvalidOperationException(
				@"OrderBy calls are not valid during map or reduce phase, but the following was found:
" + text + @"
OrderBy calls modify the indexing output, but doesn't actually impact the order of results returned from the database.
You should be calling OrderBy on the QUERY, not on the index, if you want to specify ordering.");
		}
		
		public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
		{
			switch (memberReferenceExpression.MemberName)
			{
				case "UtcNow":
				case "Now":
					var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
					if(identifierExpression != null && identifierExpression.Identifier == "DateTime")
						throw new InvalidOperationException(@"Cannot use DateTime." + memberReferenceExpression.MemberName + @" during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using DateTime." + memberReferenceExpression.MemberName +" invalidate that premise, and is not allowed");
					break;
			}


			return base.VisitMemberReferenceExpression(memberReferenceExpression, data);
		}
	}
}