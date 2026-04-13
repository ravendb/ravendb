using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes
{
    /// <summary>
    /// Roslyn syntax visitor that collects map complexity metrics from an index map function
    /// for use in computing the index heaviness static score.
    /// </summary>
    internal sealed class IndexMapComplexityVisitor : CSharpSyntaxWalker
    {
        public int LoadDocumentCount { get; private set; }
        public bool HasFanout { get; private set; }
        public bool HasNestedFanout { get; private set; }
        public int LetClauseCount { get; private set; }
        public bool HasWhereClause { get; private set; }
        public bool HasRecurse { get; private set; }

        private readonly HashSet<SyntaxNode> _visitedFanoutNodes = new();

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            AssertSufficientStack();

            string methodName = null;

            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
                methodName = memberAccess.Name.Identifier.ValueText;
            else if (node.Expression is IdentifierNameSyntax identifier)
                methodName = identifier.Identifier.ValueText;

            switch (methodName)
            {
                case "LoadDocument":
                    LoadDocumentCount++;
                    break;

                case "SelectMany":
                    TrackFanout(node);
                    break;

                case "Where":
                    HasWhereClause = true;
                    break;

                case "Recurse":
                    HasRecurse = true;
                    break;
            }

            base.VisitInvocationExpression(node);
        }

        public override void VisitQueryExpression(QueryExpressionSyntax node)
        {
            AssertSufficientStack();

            // A query expression with multiple from clauses indicates fanout
            int additionalFromCount = 0;
            foreach (var clause in node.Body.Clauses)
            {
                if (clause is FromClauseSyntax)
                    additionalFromCount++;
            }

            if (additionalFromCount > 0)
            {
                if (HasFanout)
                    HasNestedFanout = true;
                HasFanout = true;
            }

            base.VisitQueryExpression(node);
        }

        public override void VisitLetClause(LetClauseSyntax node)
        {
            AssertSufficientStack();
            LetClauseCount++;
            base.VisitLetClause(node);
        }

        public override void VisitWhereClause(WhereClauseSyntax node)
        {
            AssertSufficientStack();
            HasWhereClause = true;
            base.VisitWhereClause(node);
        }

        private void TrackFanout(SyntaxNode node)
        {
            if (_visitedFanoutNodes.Contains(node))
                return;

            _visitedFanoutNodes.Add(node);

            if (HasFanout)
                HasNestedFanout = true;

            HasFanout = true;
        }

        private void AssertSufficientStack()
        {
            if (RuntimeHelpers.TryEnsureSufficientExecutionStack() == false)
                throw new InvalidDataException("Index map expression is too complex to analyze.");
        }
    }
}
