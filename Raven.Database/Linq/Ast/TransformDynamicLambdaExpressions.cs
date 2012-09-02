using System;
using System.Collections.Generic;
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
				case "Average":
					node = ModifyLambdaForNumerics(lambdaExpression, parenthesizedlambdaExpression);
					break;
				case "Max":
				case "Min":
					node = ModifyLambdaForMinMax(lambdaExpression, parenthesizedlambdaExpression);
					break;
				case "OrderBy":
				case "OrderByDescending":
				case "GroupBy":
				case "Recurse":
				case "Select":
					node = ModifyLambdaForSelect(parenthesizedlambdaExpression, target);
					break;
				case "SelectMany":
					node = ModifyLambdaForSelectMany(lambdaExpression, parenthesizedlambdaExpression, invocationExpression);
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

		private static INode ModifyLambdaForSelect(ParenthesizedExpression parenthesizedlambdaExpression,
		                                           MemberReferenceExpression target)
		{
			var parentInvocation = target.TargetObject as InvocationExpression;
			if(parentInvocation != null)
			{
				var parentTarget = parentInvocation.TargetObject as MemberReferenceExpression;
				if(parentTarget != null && parentTarget.MemberName == "GroupBy")
				{
					return new CastExpression(new TypeReference("Func<IGrouping<dynamic,dynamic>, dynamic>"), parenthesizedlambdaExpression, CastType.Cast);
				}
			}
			return new CastExpression(new TypeReference("Func<dynamic, dynamic>"), parenthesizedlambdaExpression, CastType.Cast);
		}

		private static INode ModifyLambdaForSelectMany(LambdaExpression lambdaExpression,
		                                               ParenthesizedExpression parenthesizedlambdaExpression,
		                                               InvocationExpression invocationExpression)
		{
			INode node = lambdaExpression;
			var argPos = invocationExpression.Arguments.IndexOf(lambdaExpression);
			switch (argPos)
			{
				case 0: // first one, select the collection
					// need to enter a cast for (IEnumerable<dynamic>) on the end of the lambda body
					var selectManyExpression = new LambdaExpression
					{
						ExpressionBody =
							new CastExpression(new TypeReference("IEnumerable<dynamic>"),
							                   new ParenthesizedExpression(lambdaExpression.ExpressionBody), CastType.Cast),
						Parameters = lambdaExpression.Parameters,
					};
					node = new CastExpression(new TypeReference("Func<dynamic, IEnumerable<dynamic>>"),
					                          new ParenthesizedExpression(selectManyExpression), CastType.Cast);
					break;
				case 1: // the transformation func
					node = new CastExpression(new TypeReference("Func<dynamic, dynamic, dynamic>"), parenthesizedlambdaExpression,
					                          CastType.Cast);
					break;
			}
			return node;
		}

		private static INode ModifyLambdaForMinMax(LambdaExpression lambdaExpression,
		                                           ParenthesizedExpression parenthesizedlambdaExpression)
		{
			var node = new CastExpression(new TypeReference("Func<dynamic, IComparable>"), parenthesizedlambdaExpression, CastType.Cast);
			var castExpression = GetAsCastExpression(lambdaExpression.ExpressionBody);
			if (castExpression != null)
			{
				node = new CastExpression(new TypeReference("Func", new List<TypeReference>
				{
					new TypeReference("dynamic"),
					castExpression.CastTo
				}), parenthesizedlambdaExpression, CastType.Cast);
			}
			return node;
		}

		private static CastExpression GetAsCastExpression(Expression expressionBody)
		{
			var castExpression = expressionBody as CastExpression;
			if (castExpression != null)
				return castExpression;
			var parametrizedNode = expressionBody as ParenthesizedExpression;
			if (parametrizedNode != null)
				return GetAsCastExpression(parametrizedNode.Expression);
			return null;
		}

		private static INode ModifyLambdaForNumerics(LambdaExpression lambdaExpression,
		                                        ParenthesizedExpression parenthesizedlambdaExpression)
		{
			var castExpression = GetAsCastExpression(lambdaExpression.ExpressionBody);
			if (castExpression != null)
			{
				return new CastExpression(new TypeReference("Func", new List<TypeReference>
				{
					new TypeReference("dynamic"),
					castExpression.CastTo
				}), parenthesizedlambdaExpression, CastType.Cast);
			}
			var expression = new LambdaExpression
			{
				ExpressionBody = new CastExpression(new TypeReference("decimal",isKeyword:true), new ParenthesizedExpression(lambdaExpression.ExpressionBody), CastType.Cast),
				Parameters = lambdaExpression.Parameters
			};
			return new CastExpression(new TypeReference("Func<dynamic, decimal>"),
			                          new ParenthesizedExpression(expression), CastType.Cast);

		}
	}
}