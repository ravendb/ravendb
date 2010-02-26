using System;
using System.Collections.Generic;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.Visitors;

namespace Rhino.DivanDB.Linq
{
    public class TransformVisitor : AbstractAstTransformer
    {
        private string identifier;

        public string Name { get; set; }

        public override object VisitQueryExpressionFromClause(QueryExpressionFromClause queryExpressionFromClause, object data)
        {
            this.identifier = queryExpressionFromClause.Identifier;
            return base.VisitQueryExpressionFromClause(queryExpressionFromClause, data);
        }

        public override object VisitNamedArgumentExpression(NamedArgumentExpression namedArgumentExpression, object data)
        {
            var expression = namedArgumentExpression.Expression as MemberReferenceExpression;
            if (expression == null)
                return base.VisitNamedArgumentExpression(namedArgumentExpression, data);
            var identifierExpression = expression.TargetObject as IdentifierExpression;
            if (identifierExpression == null || identifierExpression.Identifier != identifier)
                return base.VisitNamedArgumentExpression(namedArgumentExpression, data);
            var right = new InvocationExpression(new MemberReferenceExpression(namedArgumentExpression.Expression, "Unwrap"))
            {
                Parent = namedArgumentExpression.Expression.Parent
            };
            namedArgumentExpression.Expression.Parent = right;
            namedArgumentExpression.Expression = right;
            return base.VisitNamedArgumentExpression(namedArgumentExpression, data);
        }

        public override object VisitQueryExpressionSelectClause(QueryExpressionSelectClause queryExpressionSelectClause, object data)
        {
            var createExpr = queryExpressionSelectClause.Projection as ObjectCreateExpression;
            if(createExpr != null && createExpr.IsAnonymousType)
            {
                createExpr.ObjectInitializer.CreateExpressions.Add(
                    new NamedArgumentExpression(
                        "_id",
                        new MemberReferenceExpression(new IdentifierExpression(identifier), "_id")
                        )
                    );
            }
            return base.VisitQueryExpressionSelectClause(queryExpressionSelectClause, data);
        }

        public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
        {
            var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
            if (identifierExpression == null || identifierExpression.Identifier != identifier)
                return base.VisitMemberReferenceExpression(memberReferenceExpression, data);
           
            var indexerExpression = new IndexerExpression(
                memberReferenceExpression.TargetObject,
                new List<Expression> { new PrimitiveExpression(memberReferenceExpression.MemberName, memberReferenceExpression.MemberName) });

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
            return indexerExpression;
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