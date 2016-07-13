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

            Fields = ExtractFields(anonymousObjectCreationExpressionSyntax);

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

            Fields = ExtractFields(anonymousObjectCreationExpressionSyntax);

            return node;
        }

        private HashSet<string> ExtractFields(AnonymousObjectCreationExpressionSyntax anonymousObjectCreationExpressionSyntax)
        {
            var fields = new HashSet<string>();
            for (var i = 0; i < anonymousObjectCreationExpressionSyntax.Initializers.Count; i++)
            {
                var initializer = anonymousObjectCreationExpressionSyntax.Initializers[i];
                string name;
                if (initializer.NameEquals != null)
                {
                    name = initializer.NameEquals.Name.Identifier.Text;
                }
                else
                {
                    var memberAccess = initializer.Expression as MemberAccessExpressionSyntax;
                    if (memberAccess == null)
                        throw new NotSupportedException($"Cannot extract field name from: {initializer}");

                    name = memberAccess.Name.Identifier.Text;
                }

                fields.Add(name);
            }

            return fields;
        }
    }
}