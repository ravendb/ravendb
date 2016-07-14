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

            var anonymousObjectCreationExpressionSyntax = node.DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (anonymousObjectCreationExpressionSyntax == null)
                return node;

            Fields = RewritersHelper.ExtractFields(anonymousObjectCreationExpressionSyntax);

            return node;
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            if (Fields != null)
                return node;

            var anonymousObjectCreationExpressionSyntax = node.DescendantNodes(descendIntoChildren: syntaxNode => true)
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (anonymousObjectCreationExpressionSyntax == null)
                return node;

            Fields = RewritersHelper.ExtractFields(anonymousObjectCreationExpressionSyntax);

            return node;
        }
    }
}