using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

/// <summary>
/// Rewrites method like:
/// FieldName = CreateVectorSearch(x.Textual)
/// into
/// FieldName = CreateVectorSearch("FieldName", x.Textual)
/// to provide a way to create dynamic field with vector
/// </summary>
public sealed class VectorFieldRewriter : CSharpSyntaxRewriter
{
    public static readonly VectorFieldRewriter Instance = new();
    
    public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
    {
        var expression = node.Expression.ToString();
        switch (expression)
        {
            case $"this.{nameof(StaticIndexBase.CreateVectorSearch)}":
            case $"{nameof(StaticIndexBase.CreateVectorSearch)}":
            case $"this.{nameof(StaticIndexBase.CreateVector)}":
            case $"{nameof(StaticIndexBase.CreateVector)}":
                var parent = GetAnonymousObjectMemberDeclaratorSyntax(node);
                var name = parent.NameEquals.Name.Identifier.Text;

                var identifier = SyntaxFactory.Literal(name);
                var variable = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, identifier);

                var arguments = node.ArgumentList.Arguments.Insert(0, SyntaxFactory.Argument(variable));
                return node.WithArgumentList(SyntaxFactory.ArgumentList(arguments));
        }

        return base.VisitInvocationExpression(node);
    }
    
    private static AnonymousObjectMemberDeclaratorSyntax GetAnonymousObjectMemberDeclaratorSyntax(SyntaxNode node)
    {
        var originalNode = node;

        while (node.Parent != null)
        {
            if (node.Parent is AnonymousObjectMemberDeclaratorSyntax anonymousObjectMemberDeclaratorSyntax)
                return anonymousObjectMemberDeclaratorSyntax;

            node = node.Parent;
        }

        throw new InvalidOperationException($"Could not extract vector field name from '{originalNode}'.");
    }
}
