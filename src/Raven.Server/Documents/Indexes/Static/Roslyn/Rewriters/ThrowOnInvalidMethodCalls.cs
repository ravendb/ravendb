using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Server.Documents.Indexes.Static.Roslyn.Rewriters
{
    public class ThrowOnInvalidMethodCalls : CSharpSyntaxRewriter
    {
        private static readonly List<ForbiddenMethod> ForbiddenMethods = new List<ForbiddenMethod>
        {
            new ForbiddenMethod(
                names: new[] { "Now", "UtcNow" },
                typeAliases: new[] { "DateTime", "System.DateTime", "DateTimeOffset", "System.DateTimeOffset", "SystemTime", "Raven.Client.Util.SystemTime", "Client.Util.SystemTime", "Util.SystemTime" },
                error: @"Cannot use {0} during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using {0} invalidate that premise, and is not allowed")
        };

        private SyntaxNode _root;

        public override SyntaxNode Visit(SyntaxNode node)
        {
            if (_root == null)
                _root = node;

            return base.Visit(node);
        }

        public override SyntaxNode VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            var name = node.Name.ToString();
            var method = ForbiddenMethods.FirstOrDefault(x => x.Names.Contains(name));
            if (method == null)
                return base.VisitMemberAccessExpression(node);

            var expression = node.Expression.ToString();
            var alias = method.TypeAliases.FirstOrDefault(x => x.Contains(expression));
            if (alias == null)
                return base.VisitMemberAccessExpression(node);

            throw new InvalidOperationException(string.Format(method.Error, $"{expression}.{name}"));
        }

        public override SyntaxNode VisitOrderByClause(OrderByClauseSyntax node)
        {
            var parent = node.Ancestors().FirstOrDefault(x => x.IsKind(SyntaxKind.InvocationExpression) || x.IsKind(SyntaxKind.QueryExpression));
            if (parent != _root)
                return base.VisitOrderByClause(node);

            ThrowOrderByException(node.ToString());
            return node;
        }

        private static bool TryCheckForDictionary(SyntaxNode node)
        {
            foreach (SyntaxNode leaf in node.ChildNodes())
            {
                if (leaf.IsKind(SyntaxKind.ArgumentList) == false && leaf.ToString().EndsWith("ToDictionary"))
                    return true;

                if (TryCheckForDictionary(leaf))
                    return true;
            }

            return false;
        }

        public override SyntaxNode VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            var expression = node.Expression.ToString();
            if (expression.EndsWith("OrderBy") == false && expression.EndsWith("OrderByDescending") == false)
                return base.VisitInvocationExpression(node);

            var parent = node.Ancestors().FirstOrDefault(x => x.IsKind(SyntaxKind.InvocationExpression) || x.IsKind(SyntaxKind.QueryExpression));
            if (parent != _root)
                return base.VisitInvocationExpression(node);

            if (TryCheckForDictionary(node))
                return base.VisitInvocationExpression(node);

            ThrowOrderByException(node.ToString());
            return node;
        }

        private static void ThrowOrderByException(string text)
        {
            throw new InvalidOperationException(
$@"OrderBy calls are not valid during map or reduce phase, but the following was found:
'{text}'
OrderBy calls modify the indexing output, but doesn't actually impact the order of results returned from the database.
You should be calling OrderBy on the QUERY, not on the index, if you want to specify ordering.");
        }

        private class ForbiddenMethod
        {
            public readonly string[] TypeAliases;
            public readonly string[] Names;
            public readonly string Error;

            public ForbiddenMethod(string[] names, string[] typeAliases, string error)
            {
                TypeAliases = typeAliases;
                Names = names;
                Error = error;
            }
        }
    }
}