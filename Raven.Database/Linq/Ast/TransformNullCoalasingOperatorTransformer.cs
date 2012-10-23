using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq.Ast
{
	public class TransformNullCoalasingOperatorTransformer : AbstractAstTransformer
	{
		/// <summary>
		/// We have to replace code such as:
		///		doc.FirstName ?? ""
		/// Into 
		///		doc.FirstName != null ? doc.FirstName : ""
		/// Because we use DynamicNullObject instead of null, and that preserve the null coallasing semantics.
		/// </summary>
		public override object VisitBinaryOperatorExpression(BinaryOperatorExpression binaryOperatorExpression, object data)
		{
			if(binaryOperatorExpression.Op==BinaryOperatorType.NullCoalescing)
			{
				var node = new ConditionalExpression(
					new BinaryOperatorExpression(binaryOperatorExpression.Left, BinaryOperatorType.ReferenceInequality,
					                             new PrimitiveExpression(null, null)),
					binaryOperatorExpression.Left,
					binaryOperatorExpression.Right
					);
				ReplaceCurrentNode(node);
				return null;
			}

			return base.VisitBinaryOperatorExpression(binaryOperatorExpression, data);
		}
	}
}