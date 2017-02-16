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
        public static ThrowOnInvalidMethodCalls Instance = new ThrowOnInvalidMethodCalls();

        private ThrowOnInvalidMethodCalls()
        {
        }

        private readonly List<ForbiddenMethod> _forbiddenMethods = new List<ForbiddenMethod>
        {
            new ForbiddenMethod(
                names: new[] { "Now", "UtcNow" },
                typeAliases: new[] { "DateTime", "System.DateTime", "DateTimeOffset", "System.DateTimeOffset", "SystemTime", "Raven.Client.Util.SystemTime", "Client.Util.SystemTime", "Util.SystemTime" },
                error: @"Cannot use {0} during a map or reduce phase.
The map or reduce functions must be referentially transparent, that is, for the same set of values, they always return the same results.
Using {0} invalidate that premise, and is not allowed"),
        };

        public override SyntaxNode VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            var name = node.Name.ToString();
            var method = _forbiddenMethods.FirstOrDefault(x => x.Names.Contains(name));
            if (method == null)
                return base.VisitMemberAccessExpression(node);

            var expression = node.Expression.ToString();
            var alias = method.TypeAliases.FirstOrDefault(x => x.Contains(expression));
            if (alias == null)
                return base.VisitMemberAccessExpression(node);

            throw new InvalidOperationException(string.Format(method.Error, $"{expression}.{name}"));
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