namespace Raven.Database.Linq.Ast
{
	using ICSharpCode.NRefactory.CSharp;

	internal class TransformFromClauses : DepthFirstAstVisitor<object, object>
	{
		public override object VisitQueryFromClause(QueryFromClause fromClause, object data)
		{
			Expression node;

			var expression = fromClause.Expression;
			var invocationExpression = expression as InvocationExpression;
			if (invocationExpression == null)
			{
				node = expression;
			}
			else
			{
				var target = invocationExpression.Target as MemberReferenceExpression;
				if (target == null || target.Target is IdentifierExpression)
					return base.VisitQueryFromClause(fromClause, data);

			    node = target.Target;
			}

			node.ReplaceWith(new ParenthesizedExpression(new CastExpression(new SimpleType("IEnumerable<dynamic>"), node.Clone())));

			return base.VisitQueryFromClause(fromClause, data);
		}
	}
}