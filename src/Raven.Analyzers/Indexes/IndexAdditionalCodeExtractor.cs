using System.Collections.Generic;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Indexes
{
    /// <summary>
    /// Detects whether an index ships C# for the server to compile, by writing to
    /// <c>AdditionalSources</c> or <c>AdditionalAssemblies</c> anywhere in its inheritance chain.
    /// </summary>
    /// <remarks>
    /// When an index ships its own code, a helper called from its Map or Reduce may well be
    /// translatable server-side, so RVN009 must not fire for it. The write can live in a shared base
    /// class while the Map that calls the helper lives in the derived class, and that base may be in
    /// another assembly, so this is extracted here and recorded in the index metadata rather than
    /// re-derived by the analyzer.
    /// </remarks>
    internal static class IndexAdditionalCodeExtractor
    {
        /// <summary>
        /// Whether any of <paramref name="declarations"/> writes to <c>AdditionalSources</c> or
        /// <c>AdditionalAssemblies</c>.
        /// </summary>
        public static bool ShipsServerSideCode(
            IReadOnlyList<ClassDeclarationSyntax> declarations,
            Compilation compilation)
        {
            foreach (ClassDeclarationSyntax declaration in declarations)
            {
                SemanticModel model = compilation.GetSemanticModel(declaration.SyntaxTree);
                if (ContainsAdditionalCodeWrite(declaration, model))
                    return true;
            }

            return false;
        }

        // True when <paramref name="decl"/> WRITES to AdditionalSources or AdditionalAssemblies. A write is
        // an assignment (AdditionalSources = … / this.AdditionalSources = …), an indexer populate
        // (AdditionalSources["Key"] = source), or an .Add(…) call (AdditionalSources.Add(…)). These are
        // AbstractCommonApiForIndexes properties; the symbol is resolved and confirmed to be a Raven.Client
        // member, so a bare read/null-check, a read such as AdditionalSources.Count, or an unrelated local
        // of the same name does NOT count.
        private static bool ContainsAdditionalCodeWrite(ClassDeclarationSyntax decl, SemanticModel model)
        {
            foreach (SyntaxNode node in decl.DescendantNodes())
            {
                if (node is AssignmentExpressionSyntax assignment)
                {
                    // The assignment target is the property directly, or the property behind an indexer
                    // (AdditionalSources["Key"] = source). A read on the right-hand side is never a target.
                    ExpressionSyntax target = assignment.Left is ElementAccessExpressionSyntax indexer
                        ? indexer.Expression
                        : assignment.Left;

                    if (IsAdditionalCodeProperty(SyntaxHelpers.TryGetSimpleMemberName(target), model))
                        return true;
                }

                // A populating call: AdditionalSources.Add(…) / base.AdditionalAssemblies.Add(…). Read-only
                // calls (Any/ContainsKey/…) and plain member reads (Count/Keys) are deliberately not writes.
                if (node is InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax { Name.Identifier.Text: "Add" } addCall }
                    && IsAdditionalCodeProperty(SyntaxHelpers.TryGetSimpleMemberName(addCall.Expression), model))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsAdditionalCodeProperty(SimpleNameSyntax? name, SemanticModel model)
        {
            if (name == null
                || (name.Identifier.Text != KnownTypes.AdditionalSourcesPropertyName
                    && name.Identifier.Text != KnownTypes.AdditionalAssembliesPropertyName))
            {
                return false;
            }

            // Must resolve to a member (the AbstractCommonApiForIndexes property), not an unrelated local
            // that merely shares the name. Reuse the shared Raven.Client namespace gate (exact match or a
            // nested namespace) so this rejects a user type that happens to declare its own
            // AdditionalSources property, exactly as every other Raven-type check does.
            ISymbol? symbol = model.GetSymbolInfo(name).Symbol;
            return symbol is (IPropertySymbol or IFieldSymbol)
                   && symbol.ContainingType is INamedTypeSymbol containingType
                   && SyntaxHelpers.IsInRavenClientNamespace(containingType);
        }
    }
}
