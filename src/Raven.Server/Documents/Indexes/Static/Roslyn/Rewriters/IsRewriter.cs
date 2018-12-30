using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class IsRewriter : CSharpSyntaxRewriter
    {
        public static readonly IsRewriter Instance = new IsRewriter();

        private IsRewriter()
        {
        }

        public override SyntaxNode VisitBinaryExpression(BinaryExpressionSyntax node)
        {
            if (node.IsKind(SyntaxKind.IsExpression) == false)
                return base.VisitBinaryExpression(node);

            if (ShouldApply(node) == false)
                return base.VisitBinaryExpression(node);

            var identifier = GetIdentifier(node);
            if (identifier == null)
                return base.VisitBinaryExpression(node);

            return SyntaxFactory.ParseExpression($"({identifier} is string || {identifier} is {typeof(LazyStringValue).FullName} || {identifier} is {typeof(LazyCompressedStringValue).FullName})");
        }

        private static bool ShouldApply(BinaryExpressionSyntax node)
        {
            var typeExpression = node.Right as PredefinedTypeSyntax;
            if (typeExpression == null)
                return false;

            var typeAsString = typeExpression.Keyword.ToString();
            if (string.IsNullOrWhiteSpace(typeAsString))
                return false;

            if (string.Equals(typeAsString, "string") || string.Equals(typeAsString, "String"))
                return true;

            var type = Type.GetType(typeAsString, throwOnError: false);
            if (type == null)
                return false;

            if (type == typeof(string) || type == typeof(LazyStringValue) || type == typeof(LazyCompressedStringValue))
                return true;

            return false;
        }

        private static string GetIdentifier(BinaryExpressionSyntax node)
        {
            var identifier = node.Left as IdentifierNameSyntax;
            if (identifier == null)
                return null;

            return identifier.Identifier.ToString();
        }
    }
}
