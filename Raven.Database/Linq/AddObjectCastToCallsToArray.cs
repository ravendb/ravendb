using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq
{
    public class AddObjectCastToCallsToArray : AbstractAstTransformer
    {
        public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
        {
            var identifierExpression = invocationExpression.TargetObject as IdentifierExpression;
            if(identifierExpression != null && identifierExpression.Identifier == "Array" && 
                invocationExpression.Arguments.Count == 1)
            {
                invocationExpression.Arguments[0] = new CastExpression(new TypeReference("object[]", true),
                                                                       invocationExpression.Arguments[0],
                                                                       CastType.Conversion);
            }
            return base.VisitInvocationExpression(invocationExpression, data);
        }
    }
}