using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Raven.Analyzers.Shared
{
    // Result of searching an index class and its user-defined base classes for a Map/AddMap.
    // Unknown means a base class lives in another (compiled) assembly and cannot be inspected, so
    // the analyzer must not report — the Map/AddMap may well be defined in that base.
    internal enum IndexChainSearch
    {
        Found,
        NotFound,
        Unknown
    }

    /// <summary>
    /// Walks an index class together with its user-defined base classes (up to the framework
    /// Abstract* base) to decide whether Map / AddMap is defined anywhere in the chain.
    /// </summary>
    /// <remarks>
    /// The chain searches here read constructor source, so like the field extractors they belong to the
    /// metadata generator, not to the analyzers. An analyzer reads the recorded
    /// <c>RavenIndexMetadataAttribute</c> instead. <see cref="IsReadableIn"/> is the exception and is
    /// used by both: it is what keeps every walk inside the current compilation.
    /// </remarks>
    internal static class IndexInheritanceInspector
    {
        /// <summary>
        /// Looks for a Map assignment reachable from a constructor of the index class or a user-defined
        /// base. Returns <see cref="IndexChainSearch.Unknown"/> when a base class is only available as
        /// metadata so the caller can suppress rather than report a false positive.
        /// </summary>
        public static IndexChainSearch FindMapAssignmentInChain(INamedTypeSymbol classSymbol, Compilation compilation) =>
            FindAssignmentInChain(classSymbol, compilation, KnownTypes.MapFieldName);

        public static IndexChainSearch FindAssignmentInChain(INamedTypeSymbol classSymbol, Compilation compilation, string propertyName)
        {
            // Collect every in-source class declaration in the chain, stopping at the framework base. A
            // metadata-only base cannot be inspected — the Map may well live there, so report Unknown
            // rather than a false positive.
            if (!TryCollectChainDeclarations(classSymbol, compilation, out List<ClassDeclarationSyntax> declarations))
                return IndexChainSearch.Unknown;

            foreach ((SyntaxNode body, SemanticModel model) in EnumerateConstructorReachableBodies(declarations, compilation))
            {
                if (ContainsAssignment(body, model, propertyName))
                    return IndexChainSearch.Found;
            }

            return IndexChainSearch.NotFound;
        }

        /// <summary>
        /// Yields every method-like body reachable from a constructor of the chain: each constructor body,
        /// then (transitively) the body of every method the visited bodies invoke. Candidate methods and
        /// seed constructors are gathered across the WHOLE chain and all partials, so a ctor that delegates
        /// to a base-class or other-partial helper (<c>MyIndex() { Setup(); }</c> with <c>void Setup()</c>
        /// declared elsewhere) is followed. A body no constructor ever reaches (a dead helper, a finalizer,
        /// an operator) is not yielded. Shared by the Map/Reduce search and the AddMap count so the two
        /// agree on what counts as defined in the constructor.
        /// </summary>
        private static IEnumerable<(SyntaxNode Body, SemanticModel Model)> EnumerateConstructorReachableBodies(
            List<ClassDeclarationSyntax> declarations,
            Compilation compilation)
        {
            Dictionary<string, List<MethodDeclarationSyntax>> methodsByName = new(StringComparer.Ordinal);
            Queue<(SyntaxNode Body, SemanticModel Model)> pending = new();

            foreach (ClassDeclarationSyntax decl in declarations)
            {
                SemanticModel model = compilation.GetSemanticModel(decl.SyntaxTree);

                foreach (MemberDeclarationSyntax member in decl.Members)
                {
                    if (member is MethodDeclarationSyntax method)
                    {
                        if (!methodsByName.TryGetValue(method.Identifier.Text, out List<MethodDeclarationSyntax>? overloads))
                        {
                            overloads = [];
                            methodsByName[method.Identifier.Text] = overloads;
                        }

                        overloads.Add(method);
                    }
                    else if (member is ConstructorDeclarationSyntax ctor && ctor.GetBodyNode() is SyntaxNode ctorBody)
                    {
                        pending.Enqueue((ctorBody, model));
                    }
                }
            }

            HashSet<string> visitedMethodNames = new(StringComparer.Ordinal);

            while (pending.Count > 0)
            {
                (SyntaxNode body, SemanticModel model) = pending.Dequeue();

                yield return (body, model);

                // Follow calls to methods declared anywhere in the chain/partials (Setup(), this.Setup(),
                // Setup<T>()); enqueue every overload sharing the name. DescendantNodesAndSelf so an
                // expression-bodied ctor/method (=> Setup()) is covered too. GetMethodName returns null
                // for a bare unqualified call, so fall back to the plain identifier.
                foreach (InvocationExpressionSyntax invocation in body.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    string? invokedName = SyntaxHelpers.GetMethodName(invocation);
                    if (invokedName == null && invocation.Expression is IdentifierNameSyntax invokedId)
                        invokedName = invokedId.Identifier.Text;

                    if (invokedName == null || !visitedMethodNames.Add(invokedName))
                        continue;

                    if (methodsByName.TryGetValue(invokedName, out List<MethodDeclarationSyntax>? targets))
                    {
                        foreach (MethodDeclarationSyntax target in targets)
                        {
                            if (target.GetBodyNode() is SyntaxNode targetBody)
                                pending.Enqueue((targetBody, compilation.GetSemanticModel(target.SyntaxTree)));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Collects every in-source class declaration in the chain (each type's partials), stopping at the
        /// framework base. Returns false when a non-framework base is metadata-only (compiled in another
        /// assembly) and therefore cannot be inspected; <paramref name="declarations"/> then holds the
        /// inspectable prefix gathered so far. Shared by the Map search (RVN004) and the field/stored-field
        /// extractors (RVN007/RVN008) so they all treat a base index class the same way and all bail alike
        /// when a base cannot be read.
        /// </summary>
        internal static bool TryCollectChainDeclarations(
            INamedTypeSymbol classSymbol,
            Compilation compilation,
            out List<ClassDeclarationSyntax> declarations) =>
            TryCollectChainDeclarations(classSymbol, compilation, out declarations, out _);

        /// <summary>
        /// As <see cref="TryCollectChainDeclarations(INamedTypeSymbol, Compilation, out List{ClassDeclarationSyntax})"/>,
        /// additionally reporting which base type ended the walk. <paramref name="foreignBase"/> is the
        /// first type in the chain whose source this compilation cannot read, or null when the walk
        /// reached the framework base and the whole chain was readable.
        /// </summary>
        internal static bool TryCollectChainDeclarations(
            INamedTypeSymbol classSymbol,
            Compilation compilation,
            out List<ClassDeclarationSyntax> declarations,
            out INamedTypeSymbol? foreignBase)
        {
            declarations = [];
            foreignBase = null;

            for (INamedTypeSymbol? type = classSymbol; type != null; type = type.BaseType)
            {
                if (SyntaxHelpers.IsKnownIndexBaseType(type))
                    break; // reached the framework base; it does not assign Map in user source

                if (!IsReadableIn(type, compilation))
                {
                    foreignBase = type;
                    return false;
                }

                foreach (SyntaxReference syntaxRef in type.DeclaringSyntaxReferences)
                {
                    if (syntaxRef.GetSyntax() is ClassDeclarationSyntax decl)
                        declarations.Add(decl);
                }
            }

            return true;
        }

        /// <summary>
        /// Whether <paramref name="type"/>'s source can be inspected using
        /// <paramref name="compilation"/>'s semantic models.
        /// </summary>
        /// <remarks>
        /// Having <see cref="ISymbol.DeclaringSyntaxReferences"/> is necessary but not sufficient. A
        /// project reference reaches the compiler as a compiled DLL, whose symbols carry no syntax at
        /// all; inside an IDE the same reference is a <see cref="CompilationReference"/>, whose symbols
        /// do carry syntax, but it belongs to the referenced compilation, and asking this compilation
        /// for a semantic model over a tree it does not own throws. Requiring
        /// <see cref="Compilation.ContainsSyntaxTree"/> covers both, so the walk stops at the assembly
        /// boundary in either host instead of throwing in one of them.
        /// </remarks>
        internal static bool IsReadableIn(INamedTypeSymbol type, Compilation compilation)
        {
            if (type.DeclaringSyntaxReferences.IsDefaultOrEmpty)
                return false;

            foreach (SyntaxReference syntaxRef in type.DeclaringSyntaxReferences)
            {
                if (!compilation.ContainsSyntaxTree(syntaxRef.SyntaxTree))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Counts AddMap/AddMapForAll call sites reachable from a constructor across the index class and
        /// its user-defined base classes, and reports whether any sit inside a loop, whether any is
        /// AddMapForAll, and whether a base class is metadata-only (unknown; the counts then cover only
        /// the readable prefix).
        /// </summary>
        public static (int count, bool anyInLoop, bool anyForAll, bool unknown) CountAddMapInChain(
            INamedTypeSymbol classSymbol,
            Compilation compilation)
        {
            int count = 0;
            bool anyInLoop = false;
            bool anyForAll = false;

            bool wholeChainReadable = TryCollectChainDeclarations(classSymbol, compilation, out List<ClassDeclarationSyntax> declarations);

            foreach ((SyntaxNode body, SemanticModel model) in EnumerateConstructorReachableBodies(declarations, compilation))
            {
                foreach (InvocationExpressionSyntax invocation in
                    body.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    string? methodName = SyntaxHelpers.GetMethodName(invocation);
                    if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                        continue;

                    // Confirm the method resolves to the AddMap defined on a multi-map index base.
                    if (SyntaxHelpers.GetMethodSymbol(invocation, model) is not IMethodSymbol method
                        || !SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                        continue;

                    count++;
                    if (IsInsideLoop(invocation, body))
                        anyInLoop = true;
                    if (methodName == KnownTypes.AddMapForAllMethodName)
                        anyForAll = true;
                }
            }

            return (count, anyInLoop, anyForAll, !wholeChainReadable);
        }

        private static bool ContainsAssignment(SyntaxNode node, SemanticModel model, string propertyName)
        {
            foreach (AssignmentExpressionSyntax assignment in
                node.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetSimpleMemberName(assignment.Left);
                if (nameNode == null || nameNode.Identifier.Text != propertyName)
                    continue;

                ISymbol? symbol = model.GetSymbolInfo(nameNode).Symbol;
                if (symbol is (IFieldSymbol or IPropertySymbol)
                    && SyntaxHelpers.IsDefinedOnIndexBase(symbol.ContainingType))
                {
                    return true;
                }
            }

            return false;
        }

        // True when <paramref name="node"/> is nested inside a loop construct within <paramref name="body"/>.
        private static bool IsInsideLoop(SyntaxNode node, SyntaxNode body)
        {
            for (SyntaxNode? current = node.Parent; current != null; current = current.Parent)
            {
                if (current is ForStatementSyntax
                            or ForEachStatementSyntax
                            or ForEachVariableStatementSyntax
                            or WhileStatementSyntax
                            or DoStatementSyntax)
                {
                    return true;
                }

                if (current == body)
                    break;
            }

            return false;
        }
    }
}
