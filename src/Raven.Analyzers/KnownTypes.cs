using System.Collections.Generic;
using System.Linq;

namespace Raven.Analyzers
{
    public static class KnownTypes
    {
        public const string IRavenQueryableName = "IRavenQueryable";

        public const string ProjectIntoMethodName = "ProjectInto";
        public const string SelectMethodName = "Select";

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

        // Projection behavior
        public const string CustomizeMethodName = "Customize";
        public const string ProjectionMethodName = "Projection";
        public const string ProjectionBehaviorTypeName = "ProjectionBehavior";

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

        /// <summary>
        /// Matched with Contains() to cover both AbstractSubscriptionWorker and SubscriptionWorker.
        /// </summary>
        public const string SubscriptionWorkerTypeName = "SubscriptionWorker";

        // ── Session lazy batching ──────────────────────────────────────────────────
        public const string LoadMethodName = "Load";
        public const string LoadAsyncMethodName = "LoadAsync";
    }
}
