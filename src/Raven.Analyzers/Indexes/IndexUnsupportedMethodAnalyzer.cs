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
            if (ShipsServerSideCode(classSymbol))
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
                    if (SyntaxHelpers.GetMethodSymbol(invocation, context.SemanticModel) is not IMethodSymbol method)
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
        //
        // The write may live in a shared BASE index class while the Map that calls the helper lives in the
        // derived class, so walk the whole user-defined base chain the same way IndexFieldExtractor /
        // IndexStoredFieldExtractor do. The chain walk (and its Compilation.GetSemanticModel calls) lives in
        // IndexInheritanceInspector so the analyzer itself does not trip RS1030. When a base is metadata-only
        // the chain is only partly inspectable; the inspectable prefix is enough because a metadata base
        // cannot contain a source AdditionalSources write we could read anyway.
        // Read from the index's recorded metadata rather than re-derived here: the AdditionalSources
        // write may sit in a base class in another assembly, whose constructor and member syntax this
        // compilation cannot read at all. The generator extracted it where the source was available.
        private static bool ShipsServerSideCode(INamedTypeSymbol classSymbol) =>
            IndexMetadataReader.Read(classSymbol).UsesAdditionalCode;

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
