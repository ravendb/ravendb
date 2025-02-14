using System;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NuGet.Packaging;
using Raven.Server.Documents.ETL.Providers.AI;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters;

/// <summary>
/// Rewrites method like:
/// FieldName = CreateVector(x.Textual)
/// into
/// FieldName = CreateVector("FieldName", x.Textual)
/// to provide a way to create dynamic field with vector
/// </summary>
internal sealed class VectorFieldRewriter(ReferencedCollectionsRetriever referencedCollectionsRetriever, CollectionNameRetriever collectionNameRetriever) : CSharpSyntaxRewriter(true)
{
    private readonly ReferencedCollectionsRetriever _referencedCollectionsRetriever = referencedCollectionsRetriever;
    private readonly CollectionNameRetriever _collectionNameRetriever = collectionNameRetriever;

    public bool HasVectorField { get; private set; }
    
    public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
    {
        var expression = node.Expression.ToString();
        switch (expression)
        {
            case $"this.{nameof(StaticIndexBase.CreateVector)}":
            case $"{nameof(StaticIndexBase.CreateVector)}":
                return Rewrite();
            
            case $"this.{nameof(StaticIndexBase.LoadVector)}":
            case $"{nameof(StaticIndexBase.LoadVector)}":
                _referencedCollectionsRetriever.CreateReferencedCollections();
                var names = _collectionNameRetriever.CollectionNames.Select(AiHelper.GetDocumentEmbeddingsCollectionName);
                _referencedCollectionsRetriever.ReferencedCollections.AddRange(names);
                return Rewrite();
        }

        return base.VisitInvocationExpression(node);

        SyntaxNode Rewrite()
        {
            var parent = GetAnonymousObjectMemberDeclaratorSyntax(node);
            var name = parent.NameEquals.Name.Identifier.Text;

            var identifier = SyntaxFactory.Literal(name);
            var variable = SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, identifier);

            var arguments = node.ArgumentList.Arguments.Insert(0, SyntaxFactory.Argument(variable));
            HasVectorField = true;
            return node.WithArgumentList(SyntaxFactory.ArgumentList(arguments));
        }
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
