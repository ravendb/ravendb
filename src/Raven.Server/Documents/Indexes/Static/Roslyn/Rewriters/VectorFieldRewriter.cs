using System;
using System.Collections.Generic;
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
internal sealed class VectorFieldRewriter(ReferencedCollectionsRetriever referencedCollectionsRetriever, CSharpSyntaxRewriter collectionNameRetriever, bool isMapReduce = false) : CSharpSyntaxRewriter(true)
{
    private readonly ReferencedCollectionsRetriever _referencedCollectionsRetriever = referencedCollectionsRetriever;
    private readonly CSharpSyntaxRewriter _collectionNameRetriever = collectionNameRetriever;

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
                if (isMapReduce)
                    throw new NotSupportedException($"Method {nameof(StaticIndexBase.LoadVector)} is not supported for map reduce indexes.");
                
                IEnumerable<string> names = _collectionNameRetriever switch
                {
                    CollectionNameRetriever cnr => cnr!.CollectionNames!.Select(AiHelper.GetDocumentEmbeddingsCollectionName),
                    CollectionNameRetrieverBase cnrb => cnrb.Collections.Select(n => AiHelper.GetDocumentEmbeddingsCollectionName(n.CollectionName)),
                    _ => throw new InvalidOperationException($"Unknown collection name retriever. Got: {_collectionNameRetriever.GetType().FullName}.")
                };

                _referencedCollectionsRetriever.CreateReferencedCollections();
                _referencedCollectionsRetriever.ReferencedCollections.AddRange(names.Distinct());
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
