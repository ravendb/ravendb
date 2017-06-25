using System.Collections.Generic;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    internal class IndexVisitor : CSharpSyntaxVisitor
    {
        private readonly IndexData _indexData;

        public IndexVisitor(IndexData indexData)
        {
            _indexData = indexData;
            indexData.NumberOfFromClauses = 0;
            indexData.SelectExpressions = new Dictionary<string, ExpressionSyntax>();
        }

        public override void VisitQueryExpression(QueryExpressionSyntax queryFromClause)
        {
            _indexData.FromExpression = queryFromClause.FromClause.Expression;
            _indexData.FromIdentifier = queryFromClause.FromClause.Identifier.ValueText;
            _indexData.NumberOfFromClauses++;

            VisitQueryBody(queryFromClause.Body);
        }

        public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax memberAccessExp)
        {
            base.VisitMemberAccessExpression(memberAccessExp);
            _indexData.Collection = memberAccessExp.Name.Identifier.ValueText;
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax invocationExpression)
        {
            base.VisitInvocationExpression(invocationExpression);

            var selectExpressions = new Dictionary<string, ExpressionSyntax>();
            var visitor = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            invocationExpression.Accept(visitor);

            var memberAccessExpression = invocationExpression.Expression as MemberAccessExpressionSyntax;

            if (memberAccessExpression == null)
            {
                base.VisitInvocationExpression(invocationExpression);
                return;
            }

            if (memberAccessExpression.Name.Identifier.ValueText == "Where")
                _indexData.HasWhere = true;

            _indexData.SelectExpressions = selectExpressions;
            _indexData.InvocationExpression = invocationExpression;
            _indexData.FromIdentifier = (invocationExpression.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax)?.Parameter.Identifier.ValueText;

            if (memberAccessExpression.Expression is IdentifierNameSyntax identifierNameSyntax)
            {
                _indexData.Collection = identifierNameSyntax.Identifier.ValueText;
                return;
            }

            if (memberAccessExpression.Expression is MemberAccessExpressionSyntax innerMemberAccessExpression)
            {
                _indexData.Collection = innerMemberAccessExpression.Name.Identifier.ValueText;
            }
        }

        public override void VisitQueryBody(QueryBodySyntax node)
        {
            if ((node.SelectOrGroup is SelectClauseSyntax) == false)
            {
               VisitGroupClause(node.SelectOrGroup as GroupClauseSyntax);
                return;
            }

            var selectExpressions = new Dictionary<string, ExpressionSyntax>();
            var visitor = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            node.Accept(visitor);

            _indexData.SelectExpressions = selectExpressions;
            _indexData.NumberOfSelectClauses++;
        }

        public override void VisitWhereClause(WhereClauseSyntax queryWhereClause)
        {
            base.VisitWhereClause(queryWhereClause);
            _indexData.HasWhere = true;
        }

        public override void VisitOrderByClause(OrderByClauseSyntax queryOrderClause)
        {
            base.VisitOrderByClause(queryOrderClause);
            _indexData.HasOrder = true;
        }

        public override void VisitOrdering(OrderingSyntax queryOrdering)
        {
            base.VisitOrdering(queryOrdering);
            _indexData.HasOrder = true;
        }
        public override void VisitGroupClause(GroupClauseSyntax queryGroupClause)
        {
            base.VisitGroupClause(queryGroupClause);
            _indexData.HasGroup = true;
        }
        public override void VisitLetClause(LetClauseSyntax queryLetClause)
        {
            base.VisitLetClause(queryLetClause);
            _indexData.HasLet = true;
        }
    }

}
