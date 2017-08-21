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

        public static HashSet<string> KnonwMethodsToInsepct = new HashSet<string>
        {
            "Select",
            "SelectMany",
            "Boost",
            "GroupBy",
            "OrderBy",
            "Distinct",
            "Where"
        };
        
        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            var mae = node.Expression as MemberAccessExpressionSyntax;
            if (mae == null)
                return Visit(node.Expression);

            if (KnonwMethodsToInsepct.Contains(mae.Name.Identifier.Text) == false)
                return Visit(node.Expression);

            var last = node.DescendantNodes(descendIntoChildren: syntaxNode =>
                {
                    if (syntaxNode is AnonymousObjectCreationExpressionSyntax)
                    {
                        return false;
                    }
                    return true;
                })
                .LastOrDefault(x => x.IsKind(SyntaxKind.AnonymousObjectCreationExpression)) as AnonymousObjectCreationExpressionSyntax;

            if (last != null)
                VisitAnonymousObjectCreationExpression(last);
            else
                ThrowIndexingFunctionMustReturnAnonymousObject();

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

            if (last != null)
                VisitAnonymousObjectCreationExpression(last);
            else
                ThrowIndexingFunctionMustReturnAnonymousObject();

            return node;
        }

        public override SyntaxNode VisitAnonymousObjectCreationExpression(AnonymousObjectCreationExpressionSyntax node)
        {
            if (Fields != null)
                return node;

            Fields = RewritersHelper.ExtractFields(node);

            return node;
        }

        private static void ThrowIndexingFunctionMustReturnAnonymousObject()
        {
            throw new InvalidOperationException("Indexing function must return an anonymous object");
        }
    }
}