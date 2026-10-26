using System.Collections.Generic;
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
            ImmutableArray.Create(DiagnosticDescriptors.IndexUnsupportedMethodCall);

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            // One registry per compilation, shared by every class this analyzer visits.
            context.RegisterCompilationStartAction(startCtx =>
            {
                IndexMetadataRegistry metadataRegistry = new(startCtx.Compilation);
                startCtx.RegisterSyntaxNodeAction(
                    ctx => Analyze(ctx, metadataRegistry), SyntaxKind.ClassDeclaration);
            });
        }

        private static void Analyze(SyntaxNodeAnalysisContext context, IndexMetadataRegistry metadataRegistry)
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
            if (ShipsServerSideCode(classSymbol, context.Compilation, metadataRegistry))
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

        private static bool ShipsServerSideCode(INamedTypeSymbol classSymbol, Compilation compilation, IndexMetadataRegistry metadataRegistry)
        {
            IndexMetadata metadata = metadataRegistry.Read(classSymbol);
            if (metadata.Analyzable)
                return metadata.UsesAdditionalCode;

            IndexInheritanceInspector.TryCollectChainDeclarations(classSymbol, compilation, out List<ClassDeclarationSyntax> declarations);
            return IndexAdditionalCodeExtractor.ShipsServerSideCode(declarations, compilation);
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
