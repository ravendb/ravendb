using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters.ReduceIndex
{
    public class QuerySyntaxResultsVariableNameRetriever : CSharpSyntaxRewriter, IResultsVariableNameRetriever
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

        public string ResultsVariableName { get; private set; }
    }
}