using System;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Raven.Analyzers.Indexes;
using Raven.Analyzers.Shared;

namespace Raven.Analyzers.Queries
{
    /// <summary>
    /// Reports RVN008 when a <c>ProjectInto&lt;T&gt;()</c> or <c>Select(…)</c> projection on a
    /// <c>session.Query&lt;TSource, TIndex&gt;()</c> references a field that is not retrievable
    /// under the effective <c>ProjectionBehavior</c>.
    ///
    /// Retrievability depends on behavior:
    ///   Default        → stored in index  OR  member of source document
    ///   FromIndex*     → stored in index  (no document fallback)
    ///   FromDocument*  → member of source document  (no index lookup)
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class QueryProjectionFieldAnalyzer : DiagnosticAnalyzer
    {
        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
            [DiagnosticDescriptors.QueryProjectionFieldNotRetrievable];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            context.RegisterCompilationStartAction(startCtx =>
            {
                ConcurrentDictionary<string, INamedTypeSymbol?> indexByName =
                    QueryIndexResolver.CreateIndexNameRegistry(startCtx);
                var storedFieldCache = new ConcurrentDictionary<INamedTypeSymbol, IndexStoredFieldSet>(
                    SymbolEqualityComparer.Default);
                var mapFieldCache = new ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet>(
                    SymbolEqualityComparer.Default);
                var pending = new ConcurrentBag<(InvocationExpressionSyntax Invocation, SemanticModel Model)>();
                var pendingQueries = new ConcurrentBag<(QueryExpressionSyntax Query, SemanticModel Model)>();

                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    var invocation = (InvocationExpressionSyntax)ctx.Node;
                    string? methodName = SyntaxHelpers.GetMethodName(invocation);
                    if (methodName != KnownTypes.ProjectIntoMethodName && methodName != KnownTypes.SelectMethodName)
                        return;
                    pending.Add((invocation, ctx.SemanticModel));
                }, SyntaxKind.InvocationExpression);

                // The fluent path above only sees Select/ProjectInto invocations. A projection written in
                // C# query-expression syntax (from o in session.Query<S, I>()... select new { o.X }) has a
                // SelectClauseSyntax, not a Select invocation, so it is collected and analyzed separately.
                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    pendingQueries.Add(((QueryExpressionSyntax)ctx.Node, ctx.SemanticModel));
                }, SyntaxKind.QueryExpression);

                startCtx.RegisterCompilationEndAction(endCtx =>
                {
                    foreach ((InvocationExpressionSyntax invocation, SemanticModel model) in pending)
                        AnalyzeInvocation(model, invocation, indexByName, storedFieldCache, mapFieldCache, endCtx.ReportDiagnostic);

                    foreach ((QueryExpressionSyntax query, SemanticModel model) in pendingQueries)
                        AnalyzeQueryExpression(model, query, indexByName, storedFieldCache, mapFieldCache, endCtx.ReportDiagnostic);
                });
            });
        }

        private static void AnalyzeInvocation(
            SemanticModel model,
            InvocationExpressionSyntax invocation,
            ConcurrentDictionary<string, INamedTypeSymbol?> indexByName,
            ConcurrentDictionary<INamedTypeSymbol, IndexStoredFieldSet> storedFieldCache,
            ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet> mapFieldCache,
            Action<Diagnostic> reportDiagnostic)
        {
            string? methodName = SyntaxHelpers.GetMethodName(invocation);

            // The receiver must be an IRavenQueryable<T>
            if (SyntaxHelpers.GetRavenQueryableReceiver(invocation, model) is not MemberAccessExpressionSyntax memberAccess)
                return;

            // Bail if another projection sits between this projection and the Query call: this
            // projection then operates on the intermediate projected shape, not the source document
            // / index, so checking its fields against TSource or the index stored set would produce
            // false positives. (e.g. Query<S,I>().Select(x => new {x.A}).Select(y => new {y.B}))
            if (HasInterveningProjection(memberAccess.Expression, model))
                return;

            // Walk inward through the chain to find the originating session.Query<>() call
            InvocationExpressionSyntax? queryCall = FindQueryCall(memberAccess.Expression, model);
            if (queryCall == null)
                return;

            if (!QueryIndexResolver.IsSessionQueryCall(queryCall, model))
                return;

            // Resolve the index/source field sets and effective ProjectionBehavior from the chain up to
            // (but not including) this projection. Shared with the query-expression path so both forms
            // resolve the projection context identically.
            if (!TryResolveProjectionContext(model, queryCall, memberAccess.Expression,
                    indexByName, storedFieldCache, mapFieldCache, out ProjectionFields fields))
                return;

            // Now check projected fields based on which form this is
            if (methodName == KnownTypes.ProjectIntoMethodName)
            {
                CheckProjectInto(model, invocation, fields.StoredFields, fields.SourceMembers, fields.IndexName, fields.SourceName, fields.Behavior, reportDiagnostic);
            }
            else // Select
            {
                CheckSelect(invocation, fields.StoredFields, fields.SourceMembers, fields.IndexName, fields.SourceName, fields.Behavior, reportDiagnostic);
            }
        }

        // Analyzes a projection written in C# query-expression syntax:
        //   from o in session.Query<TSource, TIndex>()[.Customize(...)] ... select new { o.Field, ... }
        // Mirrors AnalyzeInvocation for the Select form. The final select clause's first-hop members off
        // the from-clause range variable are checked against the resolved field set. A group/select ...
        // into continuation rebinds the range variable to a projected/grouped shape, so its clauses no
        // longer project the source document — bail there to avoid a false positive (the query-expression
        // analog of HasInterveningProjection).
        private static void AnalyzeQueryExpression(
            SemanticModel model,
            QueryExpressionSyntax query,
            ConcurrentDictionary<string, INamedTypeSymbol?> indexByName,
            ConcurrentDictionary<INamedTypeSymbol, IndexStoredFieldSet> storedFieldCache,
            ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet> mapFieldCache,
            Action<Diagnostic> reportDiagnostic)
        {
            if (query.Body.Continuation != null)
                return;

            if (query.Body.SelectOrGroup is not SelectClauseSyntax select)
                return;

            ExpressionSyntax sourceExpression = query.FromClause.Expression;
            if (!SyntaxHelpers.IsRavenQueryable(model.GetTypeInfo(sourceExpression).Type))
                return;

            InvocationExpressionSyntax? queryCall = FindQueryCall(sourceExpression, model);
            if (queryCall == null || !QueryIndexResolver.IsSessionQueryCall(queryCall, model))
                return;

            if (!TryResolveProjectionContext(model, queryCall, sourceExpression,
                    indexByName, storedFieldCache, mapFieldCache, out ProjectionFields fields))
                return;

            string paramName = query.FromClause.Identifier.ValueText;
            CheckProjectionBody(select.Expression, paramName, fields.StoredFields, fields.SourceMembers,
                fields.IndexName, fields.SourceName, fields.Behavior, reportDiagnostic);
        }

        // The resolved projection context: the index stored-field set (or the map projection when
        // StoreAllFields is used), the source document's public members, display names for the diagnostic
        // message, and the effective ProjectionBehavior.
        private readonly record struct ProjectionFields(
            ImmutableHashSet<string> StoredFields,
            ImmutableHashSet<string> SourceMembers,
            string IndexName,
            string SourceName,
            string Behavior);

        // Resolves the index class from the session.Query<TSource, TIndex>() call, extracts its stored /
        // map field set and the source document members, and reads the effective ProjectionBehavior from
        // the chain expression that precedes the projection. Returns false (bailing the whole check) when
        // the index cannot be resolved, the field set cannot be analyzed, or the behavior is ambiguous.
        private static bool TryResolveProjectionContext(
            SemanticModel model,
            InvocationExpressionSyntax queryCall,
            ExpressionSyntax behaviorChainExpression,
            ConcurrentDictionary<string, INamedTypeSymbol?> indexByName,
            ConcurrentDictionary<INamedTypeSymbol, IndexStoredFieldSet> storedFieldCache,
            ConcurrentDictionary<INamedTypeSymbol, IndexFieldSet> mapFieldCache,
            out ProjectionFields fields)
        {
            fields = default;

            INamedTypeSymbol? indexClass = QueryIndexResolver.ResolveIndexClass(queryCall, model, indexByName);
            if (indexClass == null)
                return false;

            INamedTypeSymbol? sourceType = QueryIndexResolver.ResolveSourceType(queryCall, model);
            if (sourceType == null)
                return false;

            // Extract stored fields from the index (bail if analysis not possible); cached per compilation
            IndexStoredFieldSet storedSet = storedFieldCache.GetOrAdd(indexClass,
                ic => IndexStoredFieldExtractor.Extract(ic, model.Compilation));
            if (storedSet.Status == StoredFieldsStatus.BailCannotAnalyze)
                return false;

            // If StoreAllFields was used, the stored set equals the map projection
            ImmutableHashSet<string> storedFields;
            if (storedSet.Status == StoredFieldsStatus.AllStored)
            {
                IndexFieldSet mapFields = mapFieldCache.GetOrAdd(indexClass,
                    ic => IndexFieldExtractor.Extract(ic, model.Compilation));
                if (mapFields.Status == IndexFieldInspection.BailCannotAnalyze)
                    return false;
                storedFields = mapFields.Fields;
            }
            else
            {
                storedFields = storedSet.Fields;
            }

            ImmutableHashSet<string> sourceMembers = SourceMemberExtractor.GetPublicMembers(sourceType);

            // Resolve the effective ProjectionBehavior from Customize(x => x.Projection(...)) in the chain
            if (TryResolveProjectionBehavior(behaviorChainExpression, model, out var behavior) == false)
                return false;
            
            fields = new ProjectionFields(storedFields, sourceMembers, indexClass.Name, sourceType.Name, behavior);
            return true;
        }

        /// <summary>
        /// Walks inward through the invocation chain to find a session.Query call.
        /// </summary>
        private static InvocationExpressionSyntax? FindQueryCall(ExpressionSyntax expression, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax inv in SyntaxHelpers.EnumerateInvocationChain(expression, model))
            {
                string? name = SyntaxHelpers.GetMethodName(inv);
                if (name == KnownTypes.QueryMethods.Query)
                    return inv;
            }
            return null;
        }

        /// <summary>
        /// Returns true when a Select/ProjectInto projection appears in <paramref name="receiver"/>'s
        /// chain before the originating Query call — meaning the analyzed projection's input shape is
        /// an intermediate projected type rather than the source document.
        /// </summary>
        private static bool HasInterveningProjection(ExpressionSyntax receiver, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax inv in SyntaxHelpers.EnumerateInvocationChain(receiver, model))
            {
                string? name = SyntaxHelpers.GetMethodName(inv);
                if (name == KnownTypes.QueryMethods.Query)
                    return false;
                if (name is KnownTypes.SelectMethodName or KnownTypes.ProjectIntoMethodName)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Walks the invocation chain looking for .Customize(x => x.Projection(ProjectionBehavior.X)).
        /// Returns the enum value name (e.g. "FromIndex"), "Default" when absent, or "bail" on ambiguity.
        /// </summary>
        private static bool TryResolveProjectionBehavior(ExpressionSyntax chainExpression, SemanticModel model, out string behavior)
        {
            // EnumerateInvocationChain yields the chain outer-to-inner, i.e. the LAST-applied call
            // first. At runtime AbstractDocumentQuery.Projection(...) is a plain assignment, so the
            // last-applied Projection wins; the first Projection customize we encounter here (the
            // outermost) is therefore the effective behavior. Return on that match rather than
            // continuing inward, which would incorrectly keep the first-applied (innermost) value.
            foreach (InvocationExpressionSyntax inv in SyntaxHelpers.EnumerateInvocationChain(chainExpression, model))
            {
                string? name = SyntaxHelpers.GetMethodName(inv);
                if (name != KnownTypes.CustomizeMethodName)
                    continue;

                SeparatedSyntaxList<ArgumentSyntax> args = inv.ArgumentList.Arguments;
                if (args.Count == 0)
                    continue;

                // Expect: x => x.Projection(ProjectionBehavior.X)
                ExpressionSyntax? lambdaBody = SyntaxHelpers.TryGetLambdaBody(args[0].Expression);
                if (lambdaBody is not InvocationExpressionSyntax projCall)
                    continue;

                string? projMethod = SyntaxHelpers.GetMethodName(projCall);
                if (projMethod != KnownTypes.ProjectionMethodName)
                    continue;

                SeparatedSyntaxList<ArgumentSyntax> projArgs = projCall.ArgumentList.Arguments;
                if (projArgs.Count == 0)
                {
                    behavior = null!;
                    return false;
                }

                // Expect: ProjectionBehavior.SomeMember
                if (projArgs[0].Expression is not MemberAccessExpressionSyntax behaviorAccess)
                {
                    behavior = null!;
                    return false; // variable or computed — bail
                }

                string typeIdent = behaviorAccess.Expression switch
                {
                    IdentifierNameSyntax id => id.Identifier.Text,
                    _ => string.Empty
                };

                if (typeIdent != KnownTypes.ProjectionBehaviorTypeName)
                {
                    behavior = null!;
                    return false;
                }

                // Outermost (last-applied) Projection customize — this is the effective behavior.
                behavior = behaviorAccess.Name.Identifier.Text;
                return true;
            }

            behavior =  KnownTypes.ProjectionBehaviorDefault;
            return true;
        }

        private static void CheckProjectInto(
            SemanticModel model,
            InvocationExpressionSyntax invocation,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string indexName,
            string sourceName,
            string behavior,
            Action<Diagnostic> reportDiagnostic)
        {
            // ProjectInto<T>() — get the type argument
            if (invocation.Expression is not MemberAccessExpressionSyntax ma)
                return;
            if (ma.Name is not GenericNameSyntax genericName)
                return;

            SeparatedSyntaxList<TypeSyntax> typeArgs = genericName.TypeArgumentList.Arguments;
            if (typeArgs.Count != 1)
                return;

            ITypeSymbol? typeArgSymbol = model.GetTypeInfo(typeArgs[0]).Type;
            if (typeArgSymbol is not INamedTypeSymbol projectionType)
                return;

            // Unresolved type argument — skip to avoid spurious diagnostics for every member
            if (projectionType.TypeKind == TypeKind.Error)
                return;

            ImmutableHashSet<string> projectionMembers = SourceMemberExtractor.GetPublicMembers(projectionType);
            Location reportLocation = typeArgs[0].GetLocation();

            foreach (string field in projectionMembers)
            {
                if (!IsFieldRetrievable(field, storedFields, sourceMembers, behavior))
                {
                    reportDiagnostic(Diagnostic.Create(
                        DiagnosticDescriptors.QueryProjectionFieldNotRetrievable,
                        reportLocation,
                        field,
                        indexName,
                        sourceName,
                        behavior));
                }
            }
        }

        private static void CheckSelect(
            InvocationExpressionSyntax invocation,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string indexName,
            string sourceName,
            string behavior,
            Action<Diagnostic> reportDiagnostic)
        {
            SeparatedSyntaxList<ArgumentSyntax> args = invocation.ArgumentList.Arguments;
            if (args.Count == 0)
                return;

            // Only handle lambda expressions
            ExpressionSyntax? lambdaBody = SyntaxHelpers.TryGetLambdaBody(args[0].Expression);
            if (lambdaBody == null)
                return;

            string? paramName = SyntaxHelpers.GetLambdaParameterName(args[0].Expression);
            if (paramName == null)
                return;

            CheckProjectionBody(lambdaBody, paramName,
                storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
        }

        // Checks a projection body (a fluent Select lambda body or a query-expression select clause) whose
        // first-hop members off <paramref name="paramName"/> are the projected fields. Handles the
        // anonymous-object and named object-initializer shapes; new Dto(...) and bare member/identifier
        // projections are intentionally not analyzed. Shared by the fluent Select and query-expression paths.
        private static void CheckProjectionBody(
            ExpressionSyntax projectionBody,
            string paramName,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string indexName,
            string sourceName,
            string behavior,
            Action<Diagnostic> reportDiagnostic)
        {
            // new { x.A, x.B } / new { X = x.A } — anonymous object
            if (projectionBody is AnonymousObjectCreationExpressionSyntax anon)
            {
                foreach (AnonymousObjectMemberDeclaratorSyntax initializer in anon.Initializers)
                {
                    CheckSelectInitializerRhs(initializer.Expression, paramName,
                        storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
                }
                return;
            }

            // new Dto { X = x.A } — named object initializer
            if (projectionBody is ObjectCreationExpressionSyntax objCreation)
            {
                CheckObjectInitializer(objCreation.Initializer, paramName,
                    storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
                return;
            }

            // new Dto(...) or x.A — bail (not analyzed)
        }

        private static void CheckObjectInitializer(
            InitializerExpressionSyntax? initializer,
            string paramName,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string indexName,
            string sourceName,
            string behavior,
            Action<Diagnostic> reportDiagnostic)
        {
            if (initializer == null || !initializer.IsKind(SyntaxKind.ObjectInitializerExpression))
                return;

            foreach (ExpressionSyntax expr in initializer.Expressions)
            {
                if (expr is not AssignmentExpressionSyntax assignment)
                    continue;

                // Check the RHS source field
                CheckSelectInitializerRhs(assignment.Right, paramName,
                    storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
            }
        }

        private static void CheckSelectInitializerRhs(
            ExpressionSyntax rhs,
            string paramName,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string indexName,
            string sourceName,
            string behavior,
            Action<Diagnostic> reportDiagnostic)
        {
            // Only check first-hop member access off the lambda parameter: x.Field
            if (rhs is not MemberAccessExpressionSyntax memberAccess)
                return;
            if (memberAccess.Expression is not IdentifierNameSyntax id)
                return;
            if (id.Identifier.ValueText != paramName)
                return;

            string fieldName = memberAccess.Name.Identifier.Text;

            // In a Select projection the LINQ provider rewrites the identity property (the default
            // convention is the member named "Id") to the document-id field id(), which is always
            // retrievable regardless of ProjectionBehavior — including FromIndex / FromIndexOrThrow.
            // (ProjectInto is different: it fetches the member name verbatim, so its Id is NOT
            // rewritten and is handled by the normal stored/source check above.) Never flag Id here.
            if (fieldName == KnownTypes.IdPropertyName)
                return;

            // Under Default behavior the field is on source doc by C# compile check → only warn for FromIndex*
            if (!IsFieldRetrievable(fieldName, storedFields, sourceMembers, behavior))
            {
                reportDiagnostic(Diagnostic.Create(
                    DiagnosticDescriptors.QueryProjectionFieldNotRetrievable,
                    memberAccess.GetLocation(),
                    fieldName,
                    indexName,
                    sourceName,
                    behavior));
            }
        }

        private static bool IsFieldRetrievable(
            string field,
            ImmutableHashSet<string> storedFields,
            ImmutableHashSet<string> sourceMembers,
            string behavior)
        {
            switch (behavior)
            {
                case KnownTypes.ProjectionBehaviorFromIndex:
                case KnownTypes.ProjectionBehaviorFromIndexOrThrow:
                    return storedFields.Contains(field);

                case KnownTypes.ProjectionBehaviorFromDocument:
                case KnownTypes.ProjectionBehaviorFromDocumentOrThrow:
                    return sourceMembers.Contains(field);

                default: // Default or unrecognized → stored OR source
                    return storedFields.Contains(field) || sourceMembers.Contains(field);
            }
        }
    }
}
