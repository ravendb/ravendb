using System.Collections.Generic;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Raven.Database.Linq
{
    public class TurnOutwardReferenceToString : AbstractAstTransformer
    {
        public override object VisitIndexerExpression(IndexerExpression indexerExpression, object data)
        {
            if (indexerExpression.Parent is IndexerExpression )
                return base.VisitIndexerExpression(indexerExpression, data);
            ReplaceCurrentNode(new InvocationExpression(
                                   new MemberReferenceExpression(indexerExpression, "ToIndexableString")
                                   ));
            return base.VisitIndexerExpression(indexerExpression, data);
        }
    }
}