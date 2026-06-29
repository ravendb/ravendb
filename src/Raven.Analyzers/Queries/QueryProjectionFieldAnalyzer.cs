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

                startCtx.RegisterSyntaxNodeAction(ctx =>
                {
                    var invocation = (InvocationExpressionSyntax)ctx.Node;
                    string? methodName = SyntaxHelpers.GetMethodName(invocation);
                    if (methodName != KnownTypes.ProjectIntoMethodName && methodName != KnownTypes.SelectMethodName)
                        return;
                    pending.Add((invocation, ctx.SemanticModel));
                }, SyntaxKind.InvocationExpression);

                startCtx.RegisterCompilationEndAction(endCtx =>
                {
                    foreach ((InvocationExpressionSyntax invocation, SemanticModel model) in pending)
                        AnalyzeInvocation(model, invocation, indexByName, storedFieldCache, mapFieldCache, endCtx.ReportDiagnostic);
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
            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            ITypeSymbol? receiverType = model.GetTypeInfo(memberAccess.Expression).Type;
            if (!SyntaxHelpers.IsRavenQueryable(receiverType))
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

            INamedTypeSymbol? indexClass = QueryIndexResolver.ResolveIndexClass(queryCall, model, indexByName);
            if (indexClass == null)
                return;

            INamedTypeSymbol? sourceType = QueryIndexResolver.ResolveSourceType(queryCall, model);
            if (sourceType == null)
                return;

            // Extract stored fields from the index (bail if analysis not possible); cached per compilation
            IndexStoredFieldSet storedSet = storedFieldCache.GetOrAdd(indexClass,
                ic => IndexStoredFieldExtractor.Extract(ic, model.Compilation));
            if (storedSet.Status == StoredFieldsStatus.BailCannotAnalyze)
                return;

            // If StoreAllFields was used, the stored set equals the map projection
            ImmutableHashSet<string> storedFields;
            if (storedSet.Status == StoredFieldsStatus.AllStored)
            {
                IndexFieldSet mapFields = mapFieldCache.GetOrAdd(indexClass,
                    ic => IndexFieldExtractor.Extract(ic, model.Compilation));
                if (mapFields.Status == IndexFieldInspection.BailCannotAnalyze)
                    return;
                storedFields = mapFields.Fields;
            }
            else
            {
                storedFields = storedSet.Fields;
            }

            ImmutableHashSet<string> sourceMembers = SourceMemberExtractor.GetPublicMembers(sourceType);

            // Resolve the effective ProjectionBehavior from Customize(x => x.Projection(...)) in the chain
            string behavior = ResolveProjectionBehavior(memberAccess.Expression, model);
            if (behavior == "bail")
                return;

            // Now check projected fields based on which form this is
            if (methodName == KnownTypes.ProjectIntoMethodName)
            {
                CheckProjectInto(model, invocation, storedFields, sourceMembers, indexClass.Name, sourceType.Name, behavior, reportDiagnostic);
            }
            else // Select
            {
                CheckSelect(invocation, storedFields, sourceMembers, indexClass.Name, sourceType.Name, behavior, reportDiagnostic);
            }
        }

        /// <summary>
        /// Walks inward through the invocation chain to find a session.Query call.
        /// </summary>
        private static InvocationExpressionSyntax? FindQueryCall(ExpressionSyntax expression, SemanticModel model)
        {
            foreach (InvocationExpressionSyntax inv in SyntaxHelpers.EnumerateInvocationChain(expression, model))
            {
                string? name = SyntaxHelpers.GetMethodName(inv);
                if (name == KnownTypes.QueryMethodName)
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
                if (name == KnownTypes.QueryMethodName)
                    return false;
                if (name == KnownTypes.SelectMethodName || name == KnownTypes.ProjectIntoMethodName)
                    return true;
            }
            return false;
        }

        /// <summary>
        /// Walks the invocation chain looking for .Customize(x => x.Projection(ProjectionBehavior.X)).
        /// Returns the enum value name (e.g. "FromIndex"), "Default" when absent, or "bail" on ambiguity.
        /// </summary>
        private static string ResolveProjectionBehavior(ExpressionSyntax chainExpression, SemanticModel model)
        {
            string result = KnownTypes.ProjectionBehaviorDefault;

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
                    return "bail";

                // Expect: ProjectionBehavior.SomeMember
                if (projArgs[0].Expression is not MemberAccessExpressionSyntax behaviorAccess)
                    return "bail"; // variable or computed — bail

                string typeIdent = behaviorAccess.Expression switch
                {
                    IdentifierNameSyntax id => id.Identifier.Text,
                    _ => string.Empty
                };

                if (typeIdent != KnownTypes.ProjectionBehaviorTypeName)
                    return "bail";

                result = behaviorAccess.Name.Identifier.Text;
                // Take the last Customize call in the chain; keep walking
            }

            return result;
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

            // Select(x => new { x.A, x.B }) — anonymous object
            if (lambdaBody is AnonymousObjectCreationExpressionSyntax anon)
            {
                foreach (AnonymousObjectMemberDeclaratorSyntax initializer in anon.Initializers)
                {
                    CheckSelectInitializerRhs(initializer.Expression, paramName,
                        storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
                }
                return;
            }

            // Select(x => new Dto { X = x.A }) — named object initializer
            if (lambdaBody is ObjectCreationExpressionSyntax objCreation)
            {
                CheckObjectInitializer(objCreation.Initializer, paramName,
                    storedFields, sourceMembers, indexName, sourceName, behavior, reportDiagnostic);
                return;
            }

            // Select(x => new Dto(...)) or Select(x => x.A) — bail (not analyzed)
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
