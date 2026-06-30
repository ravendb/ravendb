using System.Collections.Generic;
using System.Linq;

namespace Raven.Analyzers
{
    public static class KnownTypes
    {
        public const string IRavenQueryableName = "IRavenQueryable";

        public const string ProjectIntoMethodName = "ProjectInto";
        public const string SelectMethodName = nameof(Queryable.Select);
        public const string SelectManyMethodName = nameof(Queryable.SelectMany);

        /// <summary>
        /// Query chain methods that must not appear after a projection (Select / ProjectInto) in a
        /// RavenDB LINQ query. All of these operate on the source document / index shape and will
        /// silently apply to the wrong type — or throw — when the element type has been changed by
        /// a projection.
        /// </summary>
        public static readonly HashSet<string> PostProjectionForbiddenMethods =
        [
            // Standard LINQ operators that bind against the source shape
            nameof(Queryable.Where),
            nameof(Queryable.OrderBy),
            nameof(Queryable.OrderByDescending),
            nameof(Queryable.ThenBy),
            nameof(Queryable.ThenByDescending),
            nameof(Queryable.GroupBy),

            // Raven-specific operators that also bind against the source / index shape
            "Search",
            "Spatial",
            "OrderByDistance",
            "OrderByDistanceDescending",
            "OrderByScore",
            "OrderByScoreDescending",
            "ThenByScore",
            "ThenByScoreDescending",
            "MoreLikeThis",
            "VectorSearch",
            "Filter",
            "GroupByArrayValues",
            "GroupByArrayContent",
        ];

        /// <summary>
        /// Methods in an IRavenQueryable chain that accept lambda arguments and whose
        /// lambda bodies are translated server-side. User-defined method calls inside
        /// these lambdas may not survive RavenDB's expression-tree translation.
        /// </summary>
        public static readonly HashSet<string> QueryChainLambdaMethods =
        [
            nameof(Queryable.Where),
            nameof(Queryable.OrderBy),
            nameof(Queryable.OrderByDescending),
            nameof(Queryable.ThenBy),
            nameof(Queryable.ThenByDescending),
            nameof(Queryable.Select),
            nameof(Queryable.SelectMany),
            nameof(Queryable.GroupBy),
            "Search",
            "ProjectInto",
            "Filter",
            "Spatial",
            "VectorSearch",
            "OrderByDistance",
            "OrderByDistanceDescending",
            "MoreLikeThis",
            "GroupByArrayValues",
            "GroupByArrayContent",
        ];

        /// <summary>
        /// Materializing methods that consume the full query result set and are therefore
        /// unbounded without a prior .Take(n) call.
        /// </summary>
        public static readonly HashSet<string> UnboundedMaterializingMethods =
        [
            "ToList", "ToListAsync", "ToArray", "ToArrayAsync",
        ];

        // Index class names (matched by short name against base type chain)
        public const string AbstractIndexCreationTaskGenericName = "AbstractIndexCreationTask";
        public const string AbstractGenericIndexCreationTaskName = "AbstractGenericIndexCreationTask";
        public const string AbstractMultiMapIndexCreationTaskName = "AbstractMultiMapIndexCreationTask";
        public const string AbstractMultiMapTimeSeriesIndexCreationTaskName = "AbstractMultiMapTimeSeriesIndexCreationTask";
        public const string AbstractMultiMapCountersIndexCreationTaskName = "AbstractMultiMapCountersIndexCreationTask";

        public const string MapFieldName = "Map";
        public const string ReduceFieldName = "Reduce";

        public const string AddMapMethodName = "AddMap";
        public const string AddMapForAllMethodName = "AddMapForAll";

        // Session query method names
        public const string QueryMethodName = "Query";
        public const string WhereMethodName = "Where";
        public const string OrderByMethodName = "OrderBy";
        public const string OrderByDescendingMethodName = "OrderByDescending";
        public const string ThenByMethodName = "ThenBy";
        public const string ThenByDescendingMethodName = "ThenByDescending";
        public const string SearchMethodName = "Search";
        public const string TakeMethodName = "Take";

        // RavenDB client types live under this namespace. Short-name matches for the session /
        // store / queryable interfaces are gated on it (see SyntaxHelpers.IsTypeOrImplements) so the
        // analyzers stay decoupled from Raven.Client versioning — no full assembly-qualified name —
        // while not colliding with unrelated user types that happen to share a name such as
        // IDocumentSession or IRavenQueryable.
        public const string RavenClientNamespace = "Raven.Client";

        // Session type names (matched by short name)
        public const string IDocumentSessionName = "IDocumentSession";
        public const string IAsyncDocumentSessionName = "IAsyncDocumentSession";

        // JS index base (bail on dynamic field creation)
        public const string AbstractJavaScriptIndexCreationTaskName = "AbstractJavaScriptIndexCreationTask";

        // Dynamic field / bail method names
        public const string CreateFieldMethodName = "CreateField";
        public const string CreateSpatialFieldMethodName = "CreateSpatialField";
        public const string AsJsonMethodName = "AsJson";
        public const string StoreAllFieldsMethodName = "StoreAllFields";

        public const string IndexNamePropertyName = "IndexName";

        // Field storage
        public const string StoreMethodName = "Store";
        public const string StoresPropertyName = "Stores";
        public const string StoresStringsPropertyName = "StoresStrings";
        public const string FieldStorageTypeName = "FieldStorage";
        public const string FieldStorageYes = "Yes";
        public const string FieldStorageNo = "No";

        // Projection behavior
        public const string CustomizeMethodName = "Customize";
        public const string ProjectionMethodName = "Projection";
        public const string ProjectionBehaviorTypeName = "ProjectionBehavior";

        // The document identifier property (RavenDB's default id convention). In a Select projection
        // the LINQ provider rewrites a reference to it (x.Id) to the document-id field id(), which is
        // always retrievable under any ProjectionBehavior. (ProjectInto fetches member names verbatim
        // and does NOT get this rewrite, so its Id is not special-cased.) Used by RVN008.
        public const string IdPropertyName = "Id";

        // Enum value names (string form; compared with member identifier text)
        public const string ProjectionBehaviorDefault = "Default";
        public const string ProjectionBehaviorFromIndex = "FromIndex";
        public const string ProjectionBehaviorFromIndexOrThrow = "FromIndexOrThrow";
        public const string ProjectionBehaviorFromDocument = "FromDocument";
        public const string ProjectionBehaviorFromDocumentOrThrow = "FromDocumentOrThrow";

        // ── Subscription ──────────────────────────────────────────────────────────
        public const string RunMethodName = "Run";
        public const string OpenSessionMethodName = "OpenSession";
        public const string OpenAsyncSessionMethodName = "OpenAsyncSession";
        public const string IDocumentStoreName = "IDocumentStore";

        public const string SubscriptionWorkerTypeName = "SubscriptionWorker";
        public const string AbstractSubscriptionWorkerTypeName = "AbstractSubscriptionWorker";

        // The only argument type SubscriptionBatch.OpenSession / OpenAsyncSession accept besides
        // the parameterless form. The RVN011 code fix swaps the receiver but keeps the argument
        // list verbatim, so it may only offer the rewrite for an empty arg list or a single
        // SessionOptions argument — the store's OpenSession(string database) overload has no
        // batch equivalent and would not compile.
        public const string SessionOptionsTypeName = "SessionOptions";

        // ── Session lazy batching ──────────────────────────────────────────────────
        public const string LoadMethodName = "Load";
        public const string LoadAsyncMethodName = "LoadAsync";

        /// <summary>
        /// Every query method that executes a session query eagerly (materializes a result). This is
        /// the broad set used only by <c>SessionLazyBatchingAnalyzer</c>'s dependency-tracking pass to
        /// decide whether a local was produced by a materialized server call — a later operation that
        /// reads such a local genuinely depends on it and must not be batched. It deliberately includes
        /// scalar/element materializers (<c>First</c>, <c>Single</c>, <c>Count</c>, …) that the lazy
        /// rewrite cannot express; those are filtered out of the batchable set by
        /// <see cref="LazyBatchableQueryMaterializers"/>.
        /// </summary>
        public static readonly HashSet<string> SessionMaterializingMethods = new(System.StringComparer.Ordinal)
        {
            "ToList",    "ToListAsync",
            "ToArray",   "ToArrayAsync",
            "First",     "FirstAsync",
            "FirstOrDefault",  "FirstOrDefaultAsync",
            "Single",    "SingleAsync",
            "SingleOrDefault", "SingleOrDefaultAsync",
            "Any",       "AnyAsync",
            "Count",     "CountAsync",
            "LongCount", "LongCountAsync",
        };

        /// <summary>
        /// The materializers RVN012 may actually offer to batch: only <c>ToList</c>/<c>ToArray</c>
        /// (and their async forms) have a direct <c>IRavenQueryable.Lazily()</c> / <c>LazilyAsync()</c>
        /// equivalent that the code fix can rewrite to. Scalar/element materializers such as
        /// <c>First</c>, <c>Single</c>, <c>Any</c>, and <c>Count</c> would need dedicated lazy APIs and
        /// are excluded. This is the single source of truth shared by the analyzer's detection pass and
        /// the code fix's collector so the diagnostic is only raised where a working fix exists.
        /// </summary>
        public static readonly HashSet<string> LazyBatchableQueryMaterializers = new(System.StringComparer.Ordinal)
        {
            "ToList",  "ToListAsync",
            "ToArray", "ToArrayAsync",
        };
    }
}
