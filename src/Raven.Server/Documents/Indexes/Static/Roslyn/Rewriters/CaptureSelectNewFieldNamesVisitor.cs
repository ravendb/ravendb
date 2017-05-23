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

            var last = node.DescendantNodes(descendIntoChildren: syntaxNode =>
                {
                    if (syntaxNode is AnonymousObjectCreationExpressionSyntax)
                    {
                        return false;
                    }
                    return true;
                })
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (last == null)
                return node;


            VisitAnonymousObjectCreationExpression(last);

            return node;
        }

        public override SyntaxNode VisitQueryBody(QueryBodySyntax node)
        {
            if (Fields != null)
                return node;

            var last = node.DescendantNodes(descendIntoChildren: syntaxNode =>
                {
                    if (syntaxNode is AnonymousObjectCreationExpressionSyntax)
                    {
                        return false;
                    }
                    return true;
                })
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (last == null)
                return node;

            VisitAnonymousObjectCreationExpression(last);

            return node;
        }

        public override SyntaxNode VisitAnonymousObjectCreationExpression(AnonymousObjectCreationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            Fields = RewritersHelper.ExtractFields(node);

            return node;
        }
    }
}