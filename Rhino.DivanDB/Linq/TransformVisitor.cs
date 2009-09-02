using System;
using System.Collections.Generic;
using System.IO;
using ICSharpCode.NRefactory;
using ICSharpCode.NRefactory.Ast;
using ICSharpCode.NRefactory.PrettyPrinter;
using ICSharpCode.NRefactory.Visitors;

namespace Rhino.DivanDB.Linq
{
    public class TransformVisitor : AbstractAstTransformer
    {
        private string identifier;

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

        public override object VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression, object data)
        {
            var identifierExpression = memberReferenceExpression.TargetObject as IdentifierExpression;
            if (identifierExpression == null || identifierExpression.Identifier != identifier)
                return base.VisitMemberReferenceExpression(memberReferenceExpression, data);

            var indexerExpression = new IndexerExpression(
                memberReferenceExpression.TargetObject,
                new List<Expression> { new PrimitiveExpression(memberReferenceExpression.MemberName, memberReferenceExpression.MemberName) });

            ReplaceCurrentNode(indexerExpression);
            indexerExpression.Parent = memberReferenceExpression.Parent;

            return indexerExpression;
        }
    }
}