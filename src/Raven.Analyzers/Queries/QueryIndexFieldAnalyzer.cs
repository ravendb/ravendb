using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
                QueryIndexResolver.IndexNameRegistry indexByName = new(startCtx.Compilation);
                IndexMetadataRegistry metadataRegistry = new(startCtx.Compilation);

                // Registered on the operator being reported on, and on the query expression containing
                // the clauses being reported on, rather than on the Query() call they hang off.
                //
                // Two reasons. A rule reporting from RegisterCompilationEndAction is invisible in an
                // editor, which only runs the per-file actions while you type, so the diagnostic has to
                // come from a syntax node action. And a syntax node action's diagnostic has to land
                // inside the node it was registered for: registering on Query() and reporting on an
                // enclosing .Where() lambda produces the diagnostic in a full compilation pass but not
                // in the per-file pass an editor performs, which is exactly the shape that made this
                // rule show up in `dotnet build` and nowhere else.
                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    AnalyzeOperatorInvocation(
                        ctx.SemanticModel, (InvocationExpressionSyntax)ctx.Node, indexByName, metadataRegistry, ctx.ReportDiagnostic);
                }, SyntaxKind.InvocationExpression);

                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    AnalyzeQueryExpression(
                        ctx.SemanticModel, (QueryExpressionSyntax)ctx.Node, indexByName, metadataRegistry, ctx.ReportDiagnostic);
                }, SyntaxKind.QueryExpression);
            });
        }

        /// <summary>
        /// Checks one filtering or ordering operator in a fluent chain against the index it queries.
        /// </summary>
        private static void AnalyzeOperatorInvocation(
            SemanticModel model,
            InvocationExpressionSyntax invocation,
            QueryIndexResolver.IndexNameRegistry indexByName,
            IndexMetadataRegistry metadataRegistry,
            Action<Diagnostic> reportDiagnostic)
        {
            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            string methodName = memberAccess.Name.Identifier.Text;
            if (!IsFilterOrOrderMethod(methodName))
                return;

            SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
            if (args.Count == 0)
                return;

            if (!TryFindQueryCall(memberAccess.Expression, model, out InvocationExpressionSyntax? queryInvocation))
                return;

            if (!TryResolveIndexFields(queryInvocation!, model, indexByName, metadataRegistry,
                    out INamedTypeSymbol? indexClass, out ImmutableHashSet<string> mapFields))
            {
                return;
            }

            CheckLambdaFields(args[0].Expression, mapFields, methodName, indexClass!.Name, reportDiagnostic);
        }

        /// <summary>
        /// Walks down the receiver chain looking for the <c>session.Query</c> call this operator applies
        /// to, stopping at anything that changes the element type.
        /// </summary>
        /// <remarks>
        /// After a projection (Select / ProjectInto), a grouping (GroupBy, giving an IGrouping) or a
        /// fan-out (SelectMany), a subsequent Where or OrderBy binds to the new shape rather than to the
        /// source document, so its fields must not be checked against the index. Without that stop,
        /// GroupBy(o =&gt; o.Category).Where(g =&gt; g.Key == "x") would flag the IGrouping member "Key" as
        /// not indexed. An operator placed after a projection is RVN002's concern, not this rule's.
        /// </remarks>
        private static bool TryFindQueryCall(
            ExpressionSyntax receiver,
            SemanticModel model,
            out InvocationExpressionSyntax? queryInvocation)
        {
            queryInvocation = null;

            for (ExpressionSyntax? current = receiver; current != null;)
            {
                if (current is not InvocationExpressionSyntax invocation)
                    return false;

                if (QueryIndexResolver.IsSessionQueryCall(invocation, model))
                {
                    queryInvocation = invocation;
                    return true;
                }

                if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                    return false;

                string name = memberAccess.Name.Identifier.Text;
                if (name == KnownTypes.SelectMethodName
                    || name == KnownTypes.ProjectIntoMethodName
                    || name == KnownTypes.SelectManyMethodName
                    || name == KnownTypes.GroupByMethodName)
                {
                    return false;
                }

                current = memberAccess.Expression;
            }

            return false;
        }

        /// <summary>
        /// Resolves the index a <c>Query</c> call targets and its recorded map fields. False when the
        /// index cannot be resolved or its shape is not known.
        /// </summary>
        private static bool TryResolveIndexFields(
            InvocationExpressionSyntax queryInvocation,
            SemanticModel model,
            QueryIndexResolver.IndexNameRegistry indexByName,
            IndexMetadataRegistry metadataRegistry,
            out INamedTypeSymbol? indexClass,
            out ImmutableHashSet<string> mapFields)
        {
            indexClass = QueryIndexResolver.ResolveIndexClass(queryInvocation, model, indexByName);
            mapFields = ImmutableHashSet<string>.Empty;

            if (indexClass == null)
                return false;

            // The index's shape comes from the metadata its own assembly recorded, never from reading its
            // constructor here: that is what lets this rule work when the index lives in a referenced
            // project, whose constructor bodies are absent from compiled metadata entirely.
            IndexMetadata metadata = metadataRegistry.Read(indexClass);
            if (!metadata.Analyzable)
                return false;

            mapFields = metadata.MapFields;
            return true;
        }

        /// <summary>
        /// Checks the <c>where</c> / <c>orderby</c> clauses of a query written in C# query-expression
        /// syntax against the index its first <c>from</c> clause queries.
        /// </summary>
        private static void AnalyzeQueryExpression(
            SemanticModel model,
            QueryExpressionSyntax queryExpr,
            QueryIndexResolver.IndexNameRegistry indexByName,
            IndexMetadataRegistry metadataRegistry,
            Action<Diagnostic> reportDiagnostic)
        {
            if (queryExpr.FromClause.Expression is not InvocationExpressionSyntax queryInvocation)
                return;

            if (!QueryIndexResolver.IsSessionQueryCall(queryInvocation, model))
                return;

            if (!TryResolveIndexFields(queryInvocation, model, indexByName, metadataRegistry,
                    out INamedTypeSymbol? indexClass, out ImmutableHashSet<string> mapFields))
            {
                return;
            }

            AnalyzeQueryExpressionClauses(queryExpr, mapFields, indexClass!.Name, reportDiagnostic);
        }

        /// <summary>
        /// Handles the query-expression form of RVN007: when the Query() call is the source expression of
        /// a <c>from</c> clause, checks the <c>where</c>/<c>orderby</c> clauses of that query's body
        /// against the index field set, using the from-clause range variable as the lambda parameter.
        /// </summary>
        private static void AnalyzeQueryExpressionClauses(
            QueryExpressionSyntax queryExpr,
            ImmutableHashSet<string> indexedFields,
            string indexClassName,
            Action<Diagnostic> reportDiagnostic)
        {
            // Only the first from-clause introduces the source range variable that binds to the index.
            // A secondary `from` (fan-out) sits in the query body and is not the index source.
            string paramName = queryExpr.FromClause.Identifier.ValueText;

            // Only the clauses of the first query body operate on the source document / index. A
            // continuation (select … into g …) rebinds to the projected shape, so its clauses live in
            // queryExpr.Body.Continuation and are intentionally skipped — mirrors the method-chain path
            // stopping at Select/ProjectInto.
            foreach (QueryClauseSyntax clause in queryExpr.Body.Clauses)
            {
                switch (clause)
                {
                    case WhereClauseSyntax whereClause:
                        CheckFieldReferences(whereClause.Condition, paramName, indexedFields,
                            KnownTypes.QueryMethods.Where, indexClassName, reportDiagnostic);
                        break;

                    case OrderByClauseSyntax orderByClause:
                        foreach (OrderingSyntax ordering in orderByClause.Orderings)
                        {
                            CheckFieldReferences(ordering.Expression, paramName, indexedFields,
                                KnownTypes.QueryMethods.OrderBy, indexClassName, reportDiagnostic);
                        }
                        break;
                }
            }
        }

        private static bool IsFilterOrOrderMethod(string name) =>
            name is KnownTypes.QueryMethods.Where or
                KnownTypes.QueryMethods.OrderBy or
                KnownTypes.QueryMethods.OrderByDescending or
                KnownTypes.QueryMethods.ThenBy or
                KnownTypes.QueryMethods.ThenByDescending or
                KnownTypes.QueryMethods.Search;

        // True when the invocation has at least one lambda argument (l => ... or (l) => ...). Such an
        // argument introduces an inner range variable, which is the hallmark of a range-binding
        // collection operator (Any/All/Where/Select/SelectMany). Used to tell a fan-out collection hop
        // (skip the collection member) from a direct collection query such as Contains/Count (check it).
        private static bool HasLambdaArgument(InvocationExpressionSyntax invocation)
        {
            foreach (ArgumentSyntax argument in invocation.ArgumentList.Arguments)
            {
                if (argument.Expression is LambdaExpressionSyntax)
                    return true;
            }

            return false;
        }

        private static void CheckLambdaFields(
            ExpressionSyntax lambdaExpr,
            ImmutableHashSet<string> indexedFields,
            string methodName,
            string indexClassName,
            Action<Diagnostic> reportDiagnostic)
        {
            string? paramName = SyntaxHelpers.GetLambdaParameterName(lambdaExpr);
            if (paramName == null)
                return;

            ExpressionSyntax? body = SyntaxHelpers.TryGetLambdaBody(lambdaExpr);
            if (body == null)
                return;

            CheckFieldReferences(body, paramName, indexedFields, methodName, indexClassName, reportDiagnostic);
        }

        // Reports RVN007 for each first-hop member access off <paramref name="paramName"/> within
        // <paramref name="body"/> whose field is absent from the index field set. Shared by the
        // method-chain path (Where/OrderBy/Search lambda bodies) and the query-expression path (where /
        // orderby clause expressions) so both forms of a query enforce the rule identically.
        private static void CheckFieldReferences(
            ExpressionSyntax body,
            string paramName,
            ImmutableHashSet<string> indexedFields,
            string methodName,
            string indexClassName,
            Action<Diagnostic> reportDiagnostic)
        {
            // A field can be referenced several times in the same lambda (e.g. o.Price > 0 ||
            // o.Price < 0); report it only once so we don't emit duplicate diagnostics for one
            // logical issue.
            var reportedFields = new HashSet<string>(StringComparer.Ordinal);

            foreach (MemberAccessExpressionSyntax memberAccess in
                body.DescendantNodesAndSelf().OfType<MemberAccessExpressionSyntax>())
            {
                if (memberAccess.Expression is not IdentifierNameSyntax id)
                    continue;
                if (id.Identifier.ValueText != paramName)
                    continue;

                // Skip an intermediate object hop in a nested path (o.Address.City): o.Address is the
                // receiver of a further property access (o.Address.City), so its name ("Address") is
                // the object, not a queried field — checking it would be a false positive. But do NOT
                // skip a single-hop field that is the receiver of a method call (o.Tags.Contains(...))
                // or element access (o.Items[0]): there the field itself (Tags/Items) is the one being
                // queried and must still be checked. The distinction: an intermediate hop's enclosing
                // member access is a property reference, not an invocation target.
                if (memberAccess.Parent is MemberAccessExpressionSyntax parentAccess &&
                    parentAccess.Expression == memberAccess)
                {
                    // Nested property hop (o.Address.City): the enclosing access is itself the receiver
                    // of a further property reference, not an invocation target.
                    if (parentAccess.Parent is not InvocationExpressionSyntax collectionInvocation)
                        continue;

                    // Fan-out collection hop (o.Lines.Any(l => l.Product == "x")): the collection member
                    // is the receiver of a range-binding operator whose lambda introduces an inner range
                    // variable (l). A fan-out index (from o in orders from l in o.Lines select new
                    // { l.Product }) projects from that inner variable, not from the collection member,
                    // so "Lines" is legitimately absent from the field set and must not be flagged. A
                    // lambda argument is the signal that binds the inner elements; a lambda-less
                    // collection method (o.Tags.Contains(scalar), o.Tags.Count()) queries the collection
                    // field itself and stays checked below.
                    if (HasLambdaArgument(collectionInvocation))
                        continue;
                }

                string fieldName = memberAccess.Name.Identifier.Text;

                // RavenDB always exposes the document id for a static-index query (translated to the
                // id() field), regardless of whether the index Map projects Id. Mirror the sibling
                // RVN008 guard (QueryProjectionFieldAnalyzer.CheckSelectInitializerRhs) so filtering /
                // ordering by the id is never reported as not-indexed. Use continue, not return: other
                // fields referenced in the same lambda still need to be checked.
                if (fieldName == KnownTypes.IdPropertyName)
                    continue;

                if (!indexedFields.Contains(fieldName) && reportedFields.Add(fieldName))
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
    }
}
