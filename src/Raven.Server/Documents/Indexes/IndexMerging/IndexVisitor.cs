using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NetTopologySuite.GeometriesGraph;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    internal class IndexVisitor : CSharpSyntaxRewriter
    {
        private readonly IndexData _indexData;

        public IndexVisitor(IndexData indexData)
        {
            _indexData = indexData;
            indexData.NumberOfFromClauses = 0;
            indexData.SelectExpressions = new();
            _indexData.Collection = null;
        }

        public override SyntaxNode VisitQueryExpression(QueryExpressionSyntax node)
        {
            _indexData.FromExpression = node.FromClause.Expression;
            _indexData.FromIdentifier = node.FromClause.Identifier.ValueText;
            _indexData.NumberOfFromClauses++;
         //   VisitQueryBody(node.Body);


            return base.VisitQueryExpression(node);
        }

        public override SyntaxNode VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            _indexData.Collection ??= node.Name.Identifier.ValueText;
            return base.VisitMemberAccessExpression(node);
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax invocationExpression)
        {
            var selectExpressions = new Dictionary<string, ExpressionSyntax>();
            var visitor = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            invocationExpression.Accept(visitor);

            var memberAccessExpression = invocationExpression.Expression as MemberAccessExpressionSyntax;

            // select new { x = t.Select(x)
            if (memberAccessExpression == null || invocationExpression?.Parent is AnonymousObjectMemberDeclaratorSyntax)
            {
                return base.VisitInvocationExpression(invocationExpression);
            }

            if (memberAccessExpression.Name.Identifier.ValueText == "Where")
                _indexData.HasWhere = true;

            _indexData.SelectExpressions = selectExpressions;
            _indexData.InvocationExpression = invocationExpression;
            _indexData.FromIdentifier = (invocationExpression.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax)?.Parameter.Identifier.ValueText;

            if (memberAccessExpression.Expression is IdentifierNameSyntax identifierNameSyntax)
            {
                _indexData.Collection = identifierNameSyntax.Identifier.ValueText;
                return base.VisitInvocationExpression(invocationExpression);
            }

            if (memberAccessExpression.Expression is MemberAccessExpressionSyntax innerMemberAccessExpression)
            {
                _indexData.Collection = innerMemberAccessExpression.Name.Identifier.ValueText;
            }

            return base.VisitInvocationExpression(invocationExpression);
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            if ((node.SelectOrGroup is SelectClauseSyntax) == false)
            {
                return base.VisitQueryBody(node);
            }


            var selectExpressions = new Dictionary<string, ExpressionSyntax>();
            var visitor = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            node.Accept(visitor);

            _indexData.SelectExpressions = selectExpressions;
            _indexData.NumberOfSelectClauses++;
            return base.VisitQueryBody(node);
        }

        public override SyntaxNode VisitWhereClause(WhereClauseSyntax queryWhereClause)
        {
            _indexData.HasWhere = true;
            return base.VisitWhereClause(queryWhereClause);
        }

        public override SyntaxNode VisitOrderByClause(OrderByClauseSyntax queryOrderClause)
        {
            _indexData.HasOrder = true;
            return base.VisitOrderByClause(queryOrderClause);
        }

        public override SyntaxNode VisitOrdering(OrderingSyntax queryOrdering)
        {
            _indexData.HasOrder = true;
            return base.VisitOrdering(queryOrdering);
        }

        public override SyntaxNode VisitGroupClause(GroupClauseSyntax queryGroupClause)
        {
            _indexData.HasGroup = true;
            return base.VisitGroupClause(queryGroupClause);
        }

        public override SyntaxNode VisitLetClause(LetClauseSyntax queryLetClause)
        {
            _indexData.HasLet = true;
            return base.VisitLetClause(queryLetClause);
        }
    }
}
