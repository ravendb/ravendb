using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Indexes
{
    /// <summary>
    /// Reports RVN009 when a user-defined method is called inside a Map, Reduce, or AddMap lambda
    /// of a RavenDB index class. User-defined methods cannot be translated by RavenDB's
    /// expression compiler and will cause the index to fail at deployment.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class IndexUnsupportedMethodAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.IndexUnsupportedMethodCall];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(Analyze, SyntaxKind.ClassDeclaration);
        }

        private static void Analyze(SyntaxNodeAnalysisContext context)
        {
            var classDecl = (ClassDeclarationSyntax)context.Node;
            if (classDecl.BaseList == null)
                return;

            INamedTypeSymbol? classSymbol = context.SemanticModel.GetDeclaredSymbol(classDecl);
            if (classSymbol == null)
                return;

            if (!SyntaxHelpers.IsIndexCreationTask(classSymbol))
                return;

            if (SyntaxHelpers.IsJavaScriptIndex(classSymbol))
                return;

            // An index that ships user C# to the server (AdditionalSources / AdditionalAssemblies) can call
            // helper methods the server compiles and translates, so a source-defined method reference is no
            // longer a reliable "cannot be translated" signal. Suppress RVN009 for the whole class rather
            // than emit a false positive on a working index.
            if (ShipsServerSideCode(classDecl, context.SemanticModel))
                return;

            foreach (ConstructorDeclarationSyntax ctor in classDecl.Members.OfType<ConstructorDeclarationSyntax>())
            {
                SyntaxNode? body = ctor.GetBodyNode();
                if (body == null)
                    continue;

                AnalyzeCtorBody(context, body);
            }
        }

        private static void AnalyzeCtorBody(SyntaxNodeAnalysisContext context, SyntaxNode body)
        {
            foreach (SyntaxNode node in body.DescendantNodesAndSelf())
            {
                // Map, Reduce, and AddMap lambdas are all compiled server-side, so include Reduce.
                SyntaxNode? lambdaBody = SyntaxHelpers.TryGetIndexMapLambdaBody(node, context.SemanticModel, includeReduce: true);
                if (lambdaBody == null)
                    continue;

                string expressionKind = GetExpressionKind(node);

                foreach (InvocationExpressionSyntax invocation in lambdaBody.DescendantNodesAndSelf().OfType<InvocationExpressionSyntax>())
                {
                    ISymbol? symbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol;
                    if (symbol is not IMethodSymbol method)
                        continue;

                    if (!MethodTranslatabilityHelper.IsLikelyNonTranslatable(method, exemptObjectMethodOverrides: true))
                        continue;

                    Location location = SyntaxHelpers.GetInvocationNameLocation(invocation);
                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.IndexUnsupportedMethodCall,
                        location,
                        method.Name,
                        expressionKind));
                }
            }
        }

        // True when the index WRITES to AdditionalSources or AdditionalAssemblies. A write is an assignment
        // (AdditionalSources = … / this.AdditionalSources = …), an indexer populate
        // (AdditionalSources["Key"] = source), or an .Add(…) call (AdditionalSources.Add(…)). These are
        // AbstractCommonApiForIndexes properties; the symbol is resolved and confirmed to be a Raven.Client
        // member, so a bare read/null-check, a read such as AdditionalSources.Count, or an unrelated local
        // of the same name does NOT suppress. A write means the user ships C# the server compiles, so a
        // helper call in the Map/Reduce may be translatable and RVN009 must not fire.
        private static bool ShipsServerSideCode(ClassDeclarationSyntax classDecl, SemanticModel model)
        {
            foreach (SyntaxNode node in classDecl.DescendantNodes())
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

        private static string GetExpressionKind(SyntaxNode node)
        {
            // Handles bare (Reduce = …) and qualified (this.Reduce = … / base.Reduce = …) forms,
            // matching what SyntaxHelpers.ClassifyIndexMapNode accepts, so the message names the right kind.
            if (node is AssignmentExpressionSyntax assignment
                && SyntaxHelpers.TryGetSimpleMemberName(assignment.Left) is SimpleNameSyntax nameNode)
            {
                return nameNode.Identifier.Text == KnownTypes.ReduceFieldName
                    ? KnownTypes.ReduceFieldName
                    : KnownTypes.MapFieldName;
            }

            return KnownTypes.MapFieldName;
        }
    }
}
