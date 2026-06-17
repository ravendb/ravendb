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
                SyntaxNode? lambdaBody = TryGetMapLambdaBody(node, context.SemanticModel);
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
                if (SyntaxHelpers.GetMethodName(chainCall) != "SelectMany")
                    continue;

                Location loc = chainCall.Expression is MemberAccessExpressionSyntax ma
                    ? ma.Name.GetLocation()
                    : chainCall.GetLocation();

                context.ReportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.IndexFanOut, loc, "SelectMany"));
            }

            // Detect nested from clauses in query-expression form
            if (lambdaBody is QueryExpressionSyntax query)
            {
                foreach (QueryClauseSyntax clause in query.Body.Clauses)
                {
                    if (clause is not FromClauseSyntax fromClause)
                        continue;

                    context.ReportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.IndexFanOut,
                        fromClause.FromKeyword.GetLocation(),
                        fromClause.Expression.ToString()));
                }
            }
        }

        // Returns the lambda body for Map = ... / AddMap<T>(...) / AddMapForAll<T>(...).
        // Reduce is intentionally excluded: a Reduce lambda runs over already-mapped entries
        // and never produces additional outputs per source document, so it cannot fan out.
        private static SyntaxNode? TryGetMapLambdaBody(SyntaxNode node, SemanticModel model)
        {
            // Map = lambda  /  this.Map = lambda  /  base.Map = lambda
            if (node is AssignmentExpressionSyntax assignment)
            {
                SimpleNameSyntax? nameNode = SyntaxHelpers.TryGetSimpleMemberName(assignment.Left);
                if (nameNode == null || nameNode.Identifier.Text != KnownTypes.MapFieldName)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(nameNode).Symbol;
                if (sym is not (IFieldSymbol or IPropertySymbol))
                    return null;

                if (!SyntaxHelpers.IsDefinedOnIndexBase(sym.ContainingType))
                    return null;

                return SyntaxHelpers.TryGetLambdaBody(assignment.Right);
            }

            // AddMap<T>(...) or AddMapForAll<T>(...)
            if (node is InvocationExpressionSyntax invocation)
            {
                string? methodName = SyntaxHelpers.GetMethodName(invocation);
                if (methodName != KnownTypes.AddMapMethodName && methodName != KnownTypes.AddMapForAllMethodName)
                    return null;

                ISymbol? sym = model.GetSymbolInfo(invocation).Symbol;
                if (sym is not IMethodSymbol method || !SyntaxHelpers.IsMultiMapBase(method.ContainingType))
                    return null;

                SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
                if (args.Count == 0)
                    return null;

                return SyntaxHelpers.TryGetLambdaBody(args[args.Count - 1].Expression);
            }

            return null;
        }
    }
}
