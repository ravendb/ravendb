using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public abstract class ResultsVariableNameRewriter : CSharpSyntaxRewriter
    {
        public const string ResultsVariable = "results";

        protected bool _visited;

        public static ResultsVariableNameRewriter MethodSyntax => new MethodSyntaxRewriter();

        public static ResultsVariableNameRewriter QuerySyntax => new QuerySyntaxRewriter();

        private class MethodSyntaxRewriter : ResultsVariableNameRewriter
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (_visited)
                    return base.VisitInvocationExpression(node);

                _visited = true;

                var resultsIdentifer = (((node.Expression as MemberAccessExpressionSyntax) // mapResults.GroupBy(x => x.Type).Select
                                            ?.Expression as InvocationExpressionSyntax) // mapResults.GroupBy(x => x.Type)
                                            ?.Expression as MemberAccessExpressionSyntax) // mapResults.GroupBy
                                            ?.Expression as IdentifierNameSyntax; // mapResults

                if (resultsIdentifer == null)
                    return base.VisitInvocationExpression(node);

                if (ResultsVariable.Equals(resultsIdentifer.Identifier.Text, StringComparison.Ordinal))
                    return base.VisitInvocationExpression(node);

                return node.ReplaceNode(resultsIdentifer, SyntaxFactory.ParseExpression(ResultsVariable));
            }
        }

        private class QuerySyntaxRewriter : ResultsVariableNameRewriter
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (_visited)
                    return base.VisitFromClause(node);

                _visited = true;

                var resultsIdentifer = node.Expression as IdentifierNameSyntax;
                if (resultsIdentifer == null)
                    return node;

                if (ResultsVariable.Equals(resultsIdentifer.Identifier.Text, StringComparison.Ordinal))
                    return base.VisitFromClause(node);

                return node.ReplaceNode(resultsIdentifer, SyntaxFactory.ParseExpression(ResultsVariable));
            }
        }
    }
}