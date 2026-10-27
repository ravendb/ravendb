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
            ImmutableArray.Create(DiagnosticDescriptors.IndexFanOut);

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
            if (lambdaBody is not ExpressionSyntax bodyExpression)
                return;

            ReportSelectManyCalls(context, bodyExpression);

            if (SyntaxHelpers.WalkInvocationChainToRoot(bodyExpression) is QueryExpressionSyntax query)
                AnalyzeQueryExpression(context, query);
        }

        // Detect SelectMany in method-chain form
        private static void ReportSelectManyCalls(SyntaxNodeAnalysisContext context, ExpressionSyntax chain)
        {
            foreach (InvocationExpressionSyntax chainCall in SyntaxHelpers.EnumerateInvocationChain(chain))
            {
                if (SyntaxHelpers.GetMethodName(chainCall) != KnownTypes.SelectManyMethodName)
                    continue;

                Location loc = chainCall.Expression is MemberAccessExpressionSyntax ma
                    ? ma.Name.GetLocation()
                    : chainCall.GetLocation();

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.IndexFanOut, loc, KnownTypes.SelectManyMethodName));
            }
        }

        // Detect nested from clauses in query-expression form. Walk continuations too, so a
        // fan-out introduced after a 'group … into g' continuation
        // (from d in docs group d by d.X into g from item in g.Items select …) is not missed.
        private static void AnalyzeQueryExpression(SyntaxNodeAnalysisContext context, QueryExpressionSyntax query)
        {
            ReportSelectManyCalls(context, query.FromClause.Expression);

            for (QueryBodySyntax? body = query.Body; body != null; body = body.Continuation?.Body)
            {
                foreach (QueryClauseSyntax clause in body.Clauses)
                {
                    if (clause is not FromClauseSyntax fromClause)
                        continue;

                    ReportSelectManyCalls(context, fromClause.Expression);

                    if (IsTimeSeriesEntriesExpansion(fromClause.Expression, context.SemanticModel))
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

        private static bool IsTimeSeriesEntriesExpansion(ExpressionSyntax source, SemanticModel model)
        {
            if (source is not MemberAccessExpressionSyntax { Name.Identifier.ValueText: KnownTypes.TimeSeriesSegmentEntriesName } memberAccess)
                return false;

            ITypeSymbol? receiverType = model.GetTypeInfo(memberAccess.Expression).Type;
            return receiverType is { Name: KnownTypes.TimeSeriesSegmentName } && SyntaxHelpers.IsInRavenClientNamespace(receiverType);
        }
    }
}
