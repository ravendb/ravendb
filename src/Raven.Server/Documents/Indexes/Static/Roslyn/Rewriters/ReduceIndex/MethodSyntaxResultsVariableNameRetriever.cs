using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;


namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class MethodSyntaxResultsVariableNameRetriever : CSharpSyntaxRewriter, IResultsVariableNameRetriever
    {
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (ResultsVariableName != null)
                return node;

            var nodeAsString = node.Expression.ToString();

            var nodeParts = nodeAsString.Split(new[] {"."}, StringSplitOptions.RemoveEmptyEntries);
            if (nodeParts.Length <= 2)
                return node;

            ResultsVariableName = nodeParts[0];

            return node;
        }

        public string ResultsVariableName { get; private set; }
    }
}