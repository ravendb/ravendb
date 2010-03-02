using System.Collections.Generic;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Rhino.DivanDB.Linq
{
    public class TransformVisitor : AbstractAstTransformer
    {
        public string Identifier { get; set; }
        public HashSet<string> FieldNames { get; set; }

        public string Name { get; set; }

        public TransformVisitor()
        {
            FieldNames = new HashSet<string>();
        }

        public override object VisitQueryExpressionFromClause(QueryExpressionFromClause queryExpressionFromClause, object data)
        {
            Identifier = queryExpressionFromClause.Identifier;
            return base.VisitQueryExpressionFromClause(queryExpressionFromClause, data);
        }

        public override object VisitQueryExpressionSelectClause(QueryExpressionSelectClause queryExpressionSelectClause, object data)
        {
            var createExpr = queryExpressionSelectClause.Projection as ObjectCreateExpression;
            if(createExpr != null && createExpr.IsAnonymousType)
            {
                createExpr.ObjectInitializer.CreateExpressions.Add(
                    new NamedArgumentExpression(
                        "__document_id",
                        new IndexerExpression(new IndexerExpression(new IdentifierExpression(Identifier),
                            new List<Expression> { new PrimitiveExpression("@metadata", "@metadata") }),
                            new List<Expression> { new PrimitiveExpression("@id", "@id") })
                        )
                    );
            }
            return base.VisitQueryExpressionSelectClause(queryExpressionSelectClause, data);
        }

        public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
        {
            var identifierExpression = GetIdentifierExpression(memberReferenceExpression);
            if (identifierExpression == null || identifierExpression.Identifier != Identifier)
                return base.VisitMemberReferenceExpression(memberReferenceExpression, data);
           
            var indexerExpression = new IndexerExpression(
                memberReferenceExpression.TargetObject,
                new List<Expression> { new PrimitiveExpression(memberReferenceExpression.MemberName, memberReferenceExpression.MemberName) });
            FieldNames.Add(memberReferenceExpression.MemberName);

            if (ShouldAddNamedArg(memberReferenceExpression))
            {
                var namedArgumentExpression = new NamedArgumentExpression(memberReferenceExpression.MemberName,
                                                                          memberReferenceExpression);
                ReplaceCurrentNode(namedArgumentExpression);
                namedArgumentExpression.Parent = memberReferenceExpression.Parent;
                memberReferenceExpression.Parent = namedArgumentExpression;
                namedArgumentExpression.AcceptVisitor(this, data);
                namedArgumentExpression.Expression = indexerExpression;
                ReplaceCurrentNode(namedArgumentExpression);
            }
            else
            {
                ReplaceCurrentNode(indexerExpression);
            }
            indexerExpression.Parent = memberReferenceExpression.Parent;
            return base.VisitIndexerExpression(indexerExpression, data); ;
        }

        private static IdentifierExpression GetIdentifierExpression(MemberReferenceExpression memberReferenceExpression)
        {
            var referenceExpression = memberReferenceExpression.TargetObject as MemberReferenceExpression;
            if (referenceExpression != null)
                return GetIdentifierExpression(referenceExpression);
            return memberReferenceExpression.TargetObject as IdentifierExpression;
        }

        private static bool ShouldAddNamedArg(INode node)
        {
            if (node == null)
                return false;
            if(node is NamedArgumentExpression)
                return false;
            if(node is ObjectCreateExpression)
            {
                return ((ObjectCreateExpression) node).IsAnonymousType;
            }
            return ShouldAddNamedArg(node.Parent);
        }
    }
}