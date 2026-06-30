using System;
using System.Collections.Concurrent;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Raven.Analyzers.Shared
{
    /// <summary>
    /// Shared helpers for resolving the index class from a session.Query&lt;…&gt;() invocation.
    /// Used by both <c>QueryIndexFieldAnalyzer</c> and <c>QueryProjectionFieldAnalyzer</c>.
    /// </summary>
    internal static class QueryIndexResolver
    {
        /// <summary>
        /// Builds the index-name → index-class registry both query analyzers query when a string
        /// <c>indexName</c> is used. Registered once per compilation. The value is <c>null</c> when
        /// the name is <em>ambiguous</em> — two or more distinct index classes resolve to the same
        /// name (e.g. same short name in different namespaces). An ambiguous name cannot be resolved
        /// to a single field set, so it must not be validated (doing so would false-positive against
        /// the wrong index). A single registry build keeps the two analyzers in lockstep.
        /// </summary>
        internal static ConcurrentDictionary<string, INamedTypeSymbol?> CreateIndexNameRegistry(
            CompilationStartAnalysisContext startCtx)
        {
            var registry = new ConcurrentDictionary<string, INamedTypeSymbol?>(StringComparer.Ordinal);

            startCtx.RegisterSymbolAction(symCtx =>
            {
                var type = (INamedTypeSymbol)symCtx.Symbol;
                if (!SyntaxHelpers.IsIndexCreationTask(type))
                    return;

                string? key = ComputeIndexKey(type);
                if (key == null)
                    return;

                // First registration wins the slot; a second, distinct type collapses it to null
                // (ambiguous) and it stays null thereafter.
                registry.AddOrUpdate(
                    key,
                    type,
                    (_, existing) => SymbolEqualityComparer.Default.Equals(existing, type) ? existing : null);
            }, SymbolKind.NamedType);

            return registry;
        }

        /// <summary>
        /// Computes the index name an index class registers under: the overridden <c>IndexName</c>
        /// literal when present, otherwise the conventional name (type name with <c>_</c> → <c>/</c>).
        /// Returns null when the class overrides IndexName with a value that cannot be read statically,
        /// so it is left unregistered rather than registered under the wrong key.
        /// </summary>
        private static string? ComputeIndexKey(INamedTypeSymbol type)
        {
            if (TryGetOverriddenIndexNameLiteral(type, out string? overridden))
                return overridden;

            return type.Name.Replace("_", "/");
        }
        /// <summary>
        /// Returns true when the invocation is session.Query&lt;…&gt;() on IDocumentSession or IAsyncDocumentSession.
        /// </summary>
        internal static bool IsSessionQueryCall(InvocationExpressionSyntax invocation, SemanticModel model)
        {
            string? methodName = SyntaxHelpers.GetMethodName(invocation);
            if (methodName != KnownTypes.QueryMethodName)
                return false;

            ISymbol? symbol = model.GetSymbolInfo(invocation).Symbol;
            if (symbol is not IMethodSymbol method)
                return false;

            // Accept the session interfaces themselves as well as concrete implementations
            // (DocumentSession / AsyncDocumentSession), where the resolved method's containing
            // type is the class rather than the interface.
            return SyntaxHelpers.IsSessionType(method.ContainingType);
        }

        /// <summary>
        /// Resolves the index class from either the generic type argument or the indexName string literal.
        /// </summary>
        internal static INamedTypeSymbol? ResolveIndexClass(
            InvocationExpressionSyntax invocation,
            SemanticModel model,
            ConcurrentDictionary<string, INamedTypeSymbol?> indexByName)
        {
            ISymbol? symbol = model.GetSymbolInfo(invocation).Symbol;
            if (symbol is not IMethodSymbol method)
                return null;

            // Generic form: Query<T, TIndexCreator>() — two type parameters.
            // Resolved from the type argument directly; IndexName override is irrelevant here.
            if (method.TypeArguments.Length == 2)
            {
                if (method.TypeArguments[1] is INamedTypeSymbol indexType
                    && SyntaxHelpers.IsIndexCreationTask(indexType))
                {
                    return indexType;
                }
                return null;
            }

            // String form: Query<T>(indexName: "...", ...)
            if (method.TypeArguments.Length == 1)
            {
                string? literal = TryGetIndexNameLiteral(invocation);
                if (literal == null)
                    return null;

                indexByName.TryGetValue(literal, out INamedTypeSymbol? found);
                return found;
            }

            return null;
        }

        /// <summary>
        /// Extracts the source document type from the first type argument of a session.Query&lt;TSource, …&gt;() call.
        /// Returns null when the type is dynamic, object, or an open generic.
        /// </summary>
        internal static INamedTypeSymbol? ResolveSourceType(InvocationExpressionSyntax invocation, SemanticModel model)
        {
            ISymbol? symbol = model.GetSymbolInfo(invocation).Symbol;
            if (symbol is not IMethodSymbol method)
                return null;

            if (method.TypeArguments.Length < 1)
                return null;

            if (method.TypeArguments[0] is not INamedTypeSymbol sourceType)
                return null;

            // Bail on dynamic, object, or open generics
            if (sourceType.SpecialType == SpecialType.System_Object)
                return null;
            if (sourceType.IsUnboundGenericType || sourceType.TypeKind == TypeKind.TypeParameter)
                return null;

            return sourceType;
        }

        private static string? TryGetIndexNameLiteral(InvocationExpressionSyntax invocation)
        {
            SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
            if (args.Count == 0)
                return null;

            ArgumentSyntax? indexNameArg = null;
            foreach (ArgumentSyntax arg in args)
            {
                if (arg.NameColon == null)
                {
                    indexNameArg = arg;
                    break;
                }
                if (arg.NameColon.Name.Identifier.Text == "indexName")
                {
                    indexNameArg = arg;
                    break;
                }
            }

            if (indexNameArg == null)
                return null;

            if (indexNameArg.Expression is LiteralExpressionSyntax literal
                && literal.IsKind(SyntaxKind.StringLiteralExpression))
            {
                return literal.Token.ValueText;
            }

            return null;
        }

        /// <summary>
        /// Returns true when <paramref name="type"/> overrides <c>IndexName</c>.
        /// When true, <paramref name="literal"/> is the string value if the override is a simple
        /// string literal (expression-bodied or single return statement), or null when it is too
        /// complex to extract statically.
        /// Returns false when the property is not overridden (use the default name convention).
        /// </summary>
        internal static bool TryGetOverriddenIndexNameLiteral(INamedTypeSymbol type, out string? literal)
        {
            foreach (ISymbol member in type.GetMembers(KnownTypes.IndexNamePropertyName))
            {
                if (member is not IPropertySymbol prop || !prop.IsOverride)
                    continue;

                foreach (SyntaxReference syntaxRef in prop.DeclaringSyntaxReferences)
                {
                    if (syntaxRef.GetSyntax() is not PropertyDeclarationSyntax propDecl)
                        continue;

                    // Expression-bodied: => "literal"
                    if (TryExtractStringLiteral(propDecl.ExpressionBody?.Expression, out literal))
                        return true;

                    // Accessor list (get { return "literal"; } or get => "literal")
                    if (propDecl.AccessorList != null)
                    {
                        foreach (AccessorDeclarationSyntax accessor in propDecl.AccessorList.Accessors)
                        {
                            if (!accessor.IsKind(SyntaxKind.GetAccessorDeclaration))
                                continue;

                            // get => "literal"
                            if (TryExtractStringLiteral(accessor.ExpressionBody?.Expression, out literal))
                                return true;

                            // get { return "literal"; }
                            ReturnStatementSyntax? returnStmt = accessor.Body?.Statements
                                .OfType<ReturnStatementSyntax>()
                                .FirstOrDefault();
                            if (TryExtractStringLiteral(returnStmt?.Expression, out literal))
                                return true;
                        }
                    }
                }

                // Override exists but value is not extractable
                literal = null;
                return true;
            }

            literal = null;
            return false;
        }

        private static bool TryExtractStringLiteral(ExpressionSyntax? expr, out string? value)
        {
            if (expr is LiteralExpressionSyntax literal
                && literal.IsKind(SyntaxKind.StringLiteralExpression))
            {
                value = literal.Token.ValueText;
                return true;
            }
            value = null;
            return false;
        }
    }
}
