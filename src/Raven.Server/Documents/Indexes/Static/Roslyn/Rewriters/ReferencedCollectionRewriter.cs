using System;
using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    internal class ReferencedCollectionRewriter : CSharpSyntaxRewriter
    {
        public HashSet<string> ReferencedCollections;

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var loadDocument = node.Expression.ToString();
            if (loadDocument != "this.LoadDocument" && loadDocument != "LoadDocument")
                return base.VisitInvocationExpression(node);

            if (node.ArgumentList.Arguments.Count <= 1)
                return base.VisitInvocationExpression(node);

            var collectionLiteral = node.ArgumentList.Arguments[node.ArgumentList.Arguments.Count - 1].Expression as LiteralExpressionSyntax;
            if (collectionLiteral == null)
                return base.VisitInvocationExpression(node);

            if (ReferencedCollections == null)
                ReferencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            ReferencedCollections.Add(collectionLiteral.Token.Value.ToString());

            return base.VisitInvocationExpression(node);
        }
    }
}