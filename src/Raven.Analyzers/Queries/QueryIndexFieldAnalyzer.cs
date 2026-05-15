using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Indexes;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN007 when a Where/OrderBy/Search lambda on a session.Query&lt;T, TIndex&gt;() call
    /// references a field that is not present in the index's Map projection.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryIndexFieldAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.QueryFieldNotIndexed];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(startCtx =>
            {
                var indexByName = new ConcurrentDictionary<string, INamedTypeSymbol>(
                    System.StringComparer.Ordinal);
                var fieldSetCache = new ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet>(
                    SymbolEqualityComparer.Default);
                var pending = new ConcurrentBag<(InvocationExpressionSyntax Invocation, SemanticModel Model)>();

                startCtx.RegisterSymbolAction(symCtx =>
                {
                    var type = (INamedTypeSymbol)symCtx.Symbol;
                    if (!SyntaxHelpers.IsIndexCreationTask(type))
                        return;

                    string indexKey;
                    if (QueryIndexResolver.TryGetOverriddenIndexNameLiteral(type, out string? overriddenLiteral))
                    {
                        if (overriddenLiteral == null)
                            return;
                        indexKey = overriddenLiteral;
                    }
                    else
                    {
                        indexKey = type.Name.Replace("_", "/");
                    }

                    indexByName.TryAdd(indexKey, type);
                }, SymbolKind.NamedType);

                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    var invocation = (InvocationExpressionSyntax)ctx.Node;
                    if (SyntaxHelpers.GetMethodName(invocation) != KnownTypes.QueryMethodName)
                        return;
                    pending.Add((invocation, ctx.SemanticModel));
                }, SyntaxKind.InvocationExpression);

                startCtx.RegisterCompilationEndAction(endCtx =>
                {
                    foreach ((InvocationExpressionSyntax invocation, SemanticModel model) in pending)
                        AnalyzeInvocation(model, invocation, indexByName, fieldSetCache, endCtx.ReportDiagnostic);
                });
            });
        }

        private static void AnalyzeInvocation(
            SemanticModel model,
            InvocationExpressionSyntax queryInvocation,
            ConcurrentDictionary<string, INamedTypeSymbol> indexByName,
            ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet> fieldSetCache,
            Action<Diagnostic> reportDiagnostic)
        {
            if (!QueryIndexResolver.IsSessionQueryCall(queryInvocation, model))
                return;

            INamedTypeSymbol? indexClass = QueryIndexResolver.ResolveIndexClass(queryInvocation, model, indexByName);
            if (indexClass == null)
                return;

            IndexFieldSet fieldSet = fieldSetCache.GetOrAdd(indexClass,
                ic => IndexFieldExtractor.Extract(ic, model.Compilation));
            if (fieldSet.Status == IndexFieldInspection.BailCannotAnalyze)
                return;

            // Walk outward from the Query() call to find Where/OrderBy/Search clauses
            SyntaxNode current = queryInvocation;
            while (true)
            {
                if (current.Parent is not MemberAccessExpressionSyntax memberAccess)
                    break;
                if (memberAccess.Parent is not InvocationExpressionSyntax outerInvocation)
                    break;

                string methodName = memberAccess.Name.Identifier.Text;

                if (IsFilterOrOrderMethod(methodName))
                {
                    SeparatedSyntaxList<ArgumentSyntax> args = outerInvocation.ArgumentList.Arguments;
                    if (args.Count > 0)
                        CheckLambdaFields(args[0].Expression, fieldSet.Fields, methodName, indexClass.Name, reportDiagnostic);
                }

                current = outerInvocation;
            }
        }

        private static bool IsFilterOrOrderMethod(string name) =>
            name == KnownTypes.WhereMethodName
            || name == KnownTypes.OrderByMethodName
            || name == KnownTypes.OrderByDescendingMethodName
            || name == KnownTypes.ThenByMethodName
            || name == KnownTypes.ThenByDescendingMethodName
            || name == KnownTypes.SearchMethodName;

        private static void CheckLambdaFields(
            ExpressionSyntax lambdaExpr,
            ImmutableHashSet<string> indexedFields,
            string methodName,
            string indexClassName,
            Action<Diagnostic> reportDiagnostic)
        {
            string? paramName = GetLambdaParameterName(lambdaExpr);
            if (paramName == null)
                return;

            ExpressionSyntax? body = GetLambdaBodyExpression(lambdaExpr);
            if (body == null)
                return;

            foreach (MemberAccessExpressionSyntax memberAccess in
                body.DescendantNodesAndSelf().OfType<MemberAccessExpressionSyntax>())
            {
                if (memberAccess.Expression is not IdentifierNameSyntax id)
                    continue;
                if (id.Identifier.ValueText != paramName)
                    continue;

                string fieldName = memberAccess.Name.Identifier.Text;
                if (!indexedFields.Contains(fieldName))
                {
                    reportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryFieldNotIndexed,
                        memberAccess.GetLocation(),
                        fieldName,
                        methodName,
                        indexClassName));
                }
            }
        }

        private static string? GetLambdaParameterName(ExpressionSyntax expr) =>
            expr switch
            {
                SimpleLambdaExpressionSyntax simple => simple.Parameter.Identifier.ValueText,
                ParenthesizedLambdaExpressionSyntax paren when paren.ParameterList.Parameters.Count == 1
                    => paren.ParameterList.Parameters[0].Identifier.ValueText,
                _ => null
            };

        private static ExpressionSyntax? GetLambdaBodyExpression(ExpressionSyntax expr) =>
            expr switch
            {
                SimpleLambdaExpressionSyntax { Body: ExpressionSyntax e } => e,
                ParenthesizedLambdaExpressionSyntax { Body: ExpressionSyntax e } => e,
                _ => null
            };
    }
}
