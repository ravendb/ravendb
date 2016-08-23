using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class CaptureSelectNewFieldNamesVisitor : CSharpSyntaxRewriter
    {
        public HashSet<string> Fields;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            var last = node.DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (last == null)
                return node;

            // check if maybe we are nested
            var parent = last.Ancestors(ascendOutOfTrivia: true)
                .FirstOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            Fields = RewritersHelper.ExtractFields(parent ?? last);

            return node;
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            if (Fields != null)
                return node;

            var last = node.DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (last == null)
                return node;

            // check if maybe we are nested
            var parent = last.Ancestors(ascendOutOfTrivia: true)
                .FirstOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            Fields = RewritersHelper.ExtractFields(parent ?? last);

            return node;
        }
    }
}