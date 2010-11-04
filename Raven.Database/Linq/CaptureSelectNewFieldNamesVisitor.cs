using System;
using System.Collections.Generic;
using ICSharpCode.NRefactory.Ast;
using System.Linq;

namespace Raven.Database.Linq
{
    public class CaptureSelectNewFieldNamesVisitor : ICSharpCode.NRefactory.Visitors.AbstractAstVisitor
    {
        public HashSet<string> FieldNames = new HashSet<string>();

        public override object VisitQueryExpressionSelectClause(ICSharpCode.NRefactory.Ast.QueryExpressionSelectClause queryExpressionSelectClause, object data)
        {
            ProcessQuery(queryExpressionSelectClause.Projection);
            return base.VisitQueryExpressionSelectClause(queryExpressionSelectClause, data);
        }

        public override object VisitInvocationExpression(InvocationExpression invocationExpression, object data)
        {
            var memberReferenceExpression = invocationExpression.TargetObject as MemberReferenceExpression;

            if (memberReferenceExpression == null || memberReferenceExpression.MemberName != "Select")
                return base.VisitInvocationExpression(invocationExpression, data);

            if(invocationExpression.Arguments.Count != 1)
                return base.VisitInvocationExpression(invocationExpression, data);

            var lambdaExpression = invocationExpression.Arguments[0] as LambdaExpression;
            if(lambdaExpression == null)
                return base.VisitInvocationExpression(invocationExpression, data);

            ProcessQuery(lambdaExpression.ExpressionBody);

            return base.VisitInvocationExpression(invocationExpression, data);
        }

        private void ProcessQuery(Expression queryExpressionSelectClause)
        {
            var objectCreateExpression = queryExpressionSelectClause as ObjectCreateExpression;
            if (objectCreateExpression == null ||
                objectCreateExpression.IsAnonymousType == false)
                return;

            foreach (var expression in objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<NamedArgumentExpression>())
            {
                FieldNames.Add(expression.Name);
            }

            foreach (var expression in objectCreateExpression.ObjectInitializer.CreateExpressions.OfType<MemberReferenceExpression>())
            {
                FieldNames.Add(expression.MemberName);
            }
        }
    }
}