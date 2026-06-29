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
    /// Reports RVN014 when an index Map or AddMap lambda contains a fan-out operation.
    /// Fan-out occurs via SelectMany (method-chain form) or nested from clauses (query-expression form).
    /// Each detected fan-out produces multiple index entries per source document, which can
    /// significantly degrade indexing performance when collections are large.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class IndexFanOutAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.IndexFanOut];

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
                // Reduce is intentionally excluded (includeReduce: false): a Reduce lambda runs over
                // already-mapped entries and never produces additional outputs per source document.
                SyntaxNode? lambdaBody = SyntaxHelpers.TryGetIndexMapLambdaBody(node, context.SemanticModel, includeReduce: false);
                if (lambdaBody == null)
                    continue;

                AnalyzeLambdaBody(context, lambdaBody);
            }
        }

        private static void AnalyzeLambdaBody(SyntaxNodeAnalysisContext context, SyntaxNode lambdaBody)
        {
            // Detect SelectMany in method-chain form
            foreach (InvocationExpressionSyntax chainCall in SyntaxHelpers.EnumerateInvocationChain(lambdaBody as ExpressionSyntax))
            {
                if (SyntaxHelpers.GetMethodName(chainCall) != KnownTypes.SelectManyMethodName)
                    continue;

                Location loc = chainCall.Expression is MemberAccessExpressionSyntax ma
                    ? ma.Name.GetLocation()
                    : chainCall.GetLocation();

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.IndexFanOut, loc, KnownTypes.SelectManyMethodName));
            }

            // Detect nested from clauses in query-expression form. Walk continuations too, so a
            // fan-out introduced after a 'group … into g' continuation
            // (from d in docs group d by d.X into g from item in g.Items select …) is not missed.
            if (lambdaBody is QueryExpressionSyntax query)
            {
                for (QueryBodySyntax? body = query.Body; body != null; body = body.Continuation?.Body)
                {
                    foreach (QueryClauseSyntax clause in body.Clauses)
                    {
                        if (clause is not FromClauseSyntax fromClause)
                            continue;

                        // Use a stable token ("nested from") rather than the collection expression so the
                        // message reads consistently with the method-chain case ("fans out via 'SelectMany'").
                        context.ReportDiagnostic(Diagnostic.Create(
                            DiagnosticDescriptors.IndexFanOut,
                            fromClause.FromKeyword.GetLocation(),
                            "nested from"));
                    }
                }
            }
        }

    }
}
