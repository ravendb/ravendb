using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class TransformDynamicLambdaExpressions : AbstractAstTransformer
	{
		public override object VisitLambdaExpression(ICSharpCode.NRefactory.Ast.LambdaExpression lambdaExpression, object data)
		{
			var invocationExpression = lambdaExpression.Parent as InvocationExpression;
			if (invocationExpression == null)
				return base.VisitLambdaExpression(lambdaExpression, data);

			var target = invocationExpression.TargetObject as MemberReferenceExpression;
			if(target == null)
				return base.VisitLambdaExpression(lambdaExpression, data);

			INode node = lambdaExpression;
			var parenthesizedlambdaExpression = new ParenthesizedExpression(lambdaExpression);
			switch (target.MemberName)
			{
				case "Sum":
					node = new CastExpression(new TypeReference("Func<dynamic, decimal>"), parenthesizedlambdaExpression, CastType.Cast);
					break;
				case "OrderBy":
				case "OrderByDescending":
				case "GroupBy":
				case "Recurse":
				case "Select":
					node = new CastExpression(new TypeReference("Func<dynamic, dynamic>"), parenthesizedlambdaExpression, CastType.Cast);
					break;
				case "SelectMany":
					var argPos = invocationExpression.Arguments.IndexOf(lambdaExpression);
					switch (argPos)
					{
						case 0: // first one, select the collection
							// need to enter a cast for (IEnumerable<dynamic>) on the end of the lambda body
							var expression = new LambdaExpression
							{
								ExpressionBody = new CastExpression(new TypeReference("IEnumerable<dynamic>"), new ParenthesizedExpression(lambdaExpression.ExpressionBody), CastType.Cast),
								Parameters = lambdaExpression.Parameters,
							};
							node = new CastExpression(new TypeReference("Func<dynamic, IEnumerable<dynamic>>"), new ParenthesizedExpression(expression), CastType.Cast);
							break;
						case 1: // the transformation func
							node = new CastExpression(new TypeReference("Func<dynamic, dynamic, dynamic>"), parenthesizedlambdaExpression, CastType.Cast);
							break;
					}
					break;
				case "Any":
				case "all":
				case "First":
				case "FirstOrDefault":
				case "Last":
				case "LastOfDefault":
				case "Single":
				case "Where":
				case "Count":
				case "SingleOrDefault":
					node = new CastExpression(new TypeReference("Func<dynamic, bool>"), parenthesizedlambdaExpression, CastType.Cast);
				break;
			}
			ReplaceCurrentNode(node);

			return base.VisitLambdaExpression(lambdaExpression, data);
		}
	}
}