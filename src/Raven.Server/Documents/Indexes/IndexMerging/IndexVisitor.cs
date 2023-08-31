using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.IndexMerging
{
    internal sealed class IndexVisitor : CSharpSyntaxRewriter
    {
        private readonly IndexData _indexData;

        public IndexVisitor(IndexData indexData)
        {
            _indexData = indexData;
            indexData.NumberOfFromClauses = 0;
            indexData.SelectExpressions = new();
            _indexData.Collections = null;
        }

        public override SyntaxNode VisitQueryExpression(QueryExpressionSyntax node)
        {
            AssertSufficientStack();

            _indexData.FromExpression = node.FromClause.Expression;
            _indexData.FromIdentifier = node.FromClause.Identifier.ValueText;
            _indexData.NumberOfFromClauses++;
         //   VisitQueryBody(node.Body);


            return base.VisitQueryExpression(node);
        }

        public override SyntaxNode VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            AssertSufficientStack();
            return base.VisitMemberAccessExpression(node);
        }
        
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax invocationExpression)
        {
            AssertSufficientStack();

            var memberAccessExpressionSyntax = invocationExpression.Expression as MemberAccessExpressionSyntax;
            switch (memberAccessExpressionSyntax?.Name.Identifier.ValueText)
            {
                case "LoadDocument":
                    _indexData.HasLet = true;
                    return base.VisitInvocationExpression(invocationExpression);
                case "Where":
                    _indexData.HasWhere = true;
                    return base.VisitInvocationExpression(invocationExpression);
                case "SelectMany":
                    _indexData.IsFanout = true;
                    return base.VisitInvocationExpression(invocationExpression);
            }
            
            if (_indexData.NumberOfSelectClauses >= 1 || _indexData.IsFanout || _indexData.HasLet || _indexData.HasOrder || _indexData.HasWhere || _indexData.HasGroup) //skip multiselect indexes for now.
                return base.VisitInvocationExpression(invocationExpression);

            if (memberAccessExpressionSyntax?.Name.Identifier.ValueText == "Select")
                _indexData.NumberOfSelectClauses++;
            
            _indexData.InvocationExpression = invocationExpression;
            _indexData.FromIdentifier = (invocationExpression.ArgumentList.Arguments[0].Expression as SimpleLambdaExpressionSyntax)?.Parameter.Identifier.ValueText;
            
            var arguments = invocationExpression.ArgumentList.Arguments;
            var lambdaExpression = arguments[0].Expression as SimpleLambdaExpressionSyntax;

            var expressionSyntaxes = new Dictionary<string, ExpressionSyntax>();
            var evaluator = new CaptureSelectExpressionsAndNewFieldNamesVisitor(false, new(), expressionSyntaxes);
            switch (lambdaExpression!.ExpressionBody)
            {
                case AnonymousObjectCreationExpressionSyntax aoces:
                    evaluator.VisitAnonymousObjectCreationExpression(aoces);
                    break;
            }

            _indexData.SelectExpressions = expressionSyntaxes;
            
            return base.VisitInvocationExpression(invocationExpression);
        }
        
        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            AssertSufficientStack();
            
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
            AssertSufficientStack();
            _indexData.HasWhere = true;
            return base.VisitWhereClause(queryWhereClause);
        }

        public override SyntaxNode VisitOrderByClause(OrderByClauseSyntax queryOrderClause)
        {
            AssertSufficientStack();
            _indexData.HasOrder = true;
            return base.VisitOrderByClause(queryOrderClause);
        }

        public override SyntaxNode VisitOrdering(OrderingSyntax queryOrdering)
        {
            AssertSufficientStack();
            _indexData.HasOrder = true;
            return base.VisitOrdering(queryOrdering);
        }

        public override SyntaxNode VisitGroupClause(GroupClauseSyntax queryGroupClause)
        {
            AssertSufficientStack();
            _indexData.HasGroup = true;
            return base.VisitGroupClause(queryGroupClause);
        }

        public override SyntaxNode VisitLetClause(LetClauseSyntax queryLetClause)
        {
            AssertSufficientStack();
            _indexData.HasLet = true;
            return base.VisitLetClause(queryLetClause);
        }

        private void AssertSufficientStack()
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
                throw new InvalidDataException($"Index is too complex for {nameof(IndexMerger)}.");
        }
    }
}
