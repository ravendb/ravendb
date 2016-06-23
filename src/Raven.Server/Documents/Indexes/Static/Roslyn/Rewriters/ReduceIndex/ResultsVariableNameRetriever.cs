using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public abstract class ResultsVariableNameRetriever : CSharpSyntaxRewriter
    {
        public string ResultsVariableName { get; protected set; }

        public static ResultsVariableNameRetriever MethodSyntax => new MethodSyntaxRetriever();

        public static ResultsVariableNameRetriever QuerySyntax => new QuerySyntaxRetriever();

        private class MethodSyntaxRetriever : ResultsVariableNameRetriever
        {
            public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
            {
                if (ResultsVariableName != null)
                    return node;

                var nodeAsString = node.Expression.ToString();

                var nodeParts = nodeAsString.Split(new[] { "." }, StringSplitOptions.RemoveEmptyEntries);
                if (nodeParts.Length <= 2)
                    return node;

                ResultsVariableName = nodeParts[0];

                return node;
            }
        }

        private class QuerySyntaxRetriever : ResultsVariableNameRetriever
        {
            public override SyntaxNode VisitFromClause(FromClauseSyntax node)
            {
                if (ResultsVariableName != null)
                    return node;

                var resultsIdentifer = node.Expression as IdentifierNameSyntax;
                if (resultsIdentifer == null)
                    return node;

                ResultsVariableName = resultsIdentifer.Identifier.Text;

                return node;
            }
        }
    }
}