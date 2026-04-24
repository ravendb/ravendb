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
            ImmutableArray.Create(DiagnosticDescriptors.QueryFieldNotIndexed);

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(startCtx =>
            {
                var indexByName = new ConcurrentDictionary<string, INamedTypeSymbol>(
                    System.StringComparer.Ordinal);

                startCtx.RegisterSymbolAction(symCtx =>
                {
                    var type = (INamedTypeSymbol)symCtx.Symbol;
                    if (!SyntaxHelpers.IsIndexCreationTask(type))
                        return;

                    string indexKey;
                    if (QueryIndexResolver.TryGetOverriddenIndexNameLiteral(type, out string? overriddenLiteral))
                    {
                        if (overriddenLiteral == null)
                            return; // Override exists but is not a simple string literal — skip
                        indexKey = overriddenLiteral;
                    }
                    else
                    {
                        // Default convention: GetType().Name.Replace("_", "/")
                        indexKey = type.Name.Replace("_", "/");
                    }

                    indexByName.TryAdd(indexKey, type);
                }, SymbolKind.NamedType);

                startCtx.RegisterSyntaxNodeAction(
                    ctx => AnalyzeInvocation(ctx, indexByName),
                    SyntaxKind.InvocationExpression);
            });
        }

        private static void AnalyzeInvocation(
            SyntaxNodeAnalysisContext context,
            ConcurrentDictionary<string, INamedTypeSymbol> indexByName)
        {
            var queryInvocation = (InvocationExpressionSyntax)context.Node;

            if (!QueryIndexResolver.IsSessionQueryCall(queryInvocation, context.SemanticModel))
                return;

            INamedTypeSymbol? indexClass = QueryIndexResolver.ResolveIndexClass(queryInvocation, context.SemanticModel, indexByName);
            if (indexClass == null)
                return;

            IndexFieldSet fieldSet = IndexFieldExtractor.Extract(indexClass, context.SemanticModel.Compilation);
            if (fieldSet.Status == IndexFieldInspection.BailCannotAnalyze)
                return;

            // Walk outward from the Query() call to find Where/OrderBy/Search clauses
            SyntaxNode current = queryInvocation;
            while (true)
            {
                // Pattern: (MemberAccess (Invocation ...))
                if (current.Parent is not MemberAccessExpressionSyntax memberAccess)
                    break;
                if (memberAccess.Parent is not InvocationExpressionSyntax outerInvocation)
                    break;

                string methodName = memberAccess.Name.Identifier.Text;

                if (IsFilterOrOrderMethod(methodName))
                {
                    SeparatedSyntaxList<ArgumentSyntax> args = outerInvocation.ArgumentList.Arguments;
                    // For all filter/order methods the field selector lambda is the first argument
                    if (args.Count > 0)
                        CheckLambdaFields(context, args[0].Expression, fieldSet.Fields, methodName, indexClass.Name);
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
            SyntaxNodeAnalysisContext context,
            ExpressionSyntax lambdaExpr,
            ImmutableHashSet<string> indexedFields,
            string methodName,
            string indexClassName)
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
                // Only first-hop accesses off the lambda parameter: x.Field
                // x.Field.Sub has expression x.Field (MemberAccess, not IdentifierName) → skipped automatically
                if (memberAccess.Expression is not IdentifierNameSyntax id)
                    continue;
                if (id.Identifier.ValueText != paramName)
                    continue;

                string fieldName = memberAccess.Name.Identifier.Text;
                if (!indexedFields.Contains(fieldName))
                {
                    context.ReportDiagnostic(Diagnostic.Create(
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
                SimpleLambdaExpressionSyntax simple when simple.Body is ExpressionSyntax e => e,
                ParenthesizedLambdaExpressionSyntax paren when paren.Body is ExpressionSyntax e => e,
                _ => null
            };
    }
}
