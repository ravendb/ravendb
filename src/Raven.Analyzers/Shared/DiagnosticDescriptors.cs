using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    internal static class DiagnosticDescriptors
    {
        private const string Category = "Usage";
        private const string HelpLinkBase = "https://ravendb.net/docs/analyzers/";

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryFilteringAfterProjection"/>.
        /// Fires when a filtering or ordering method appears after a projection in a RavenDB query chain.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryFilteringAfterProjection = new(
            id: DiagnosticIds.QueryFilteringAfterProjection,
            title: "RavenDB query operator after projection",
            messageFormat: "'{0}' is called after a projection (.ProjectInto or .Select). Move the projection to the end of the query chain.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB query operators that bind against the source document or index shape (Where, OrderBy, Search, Spatial, OrderByDistance, OrderByScore, MoreLikeThis, VectorSearch, Filter, GroupBy, etc.) must appear before projection (ProjectInto, Select). Projection changes the element type; any operator placed after it operates on the projected shape rather than the source document, producing silent incorrect results or a runtime exception.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.QueryFilteringAfterProjection);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.DoubleProjectInto"/>.
        /// Fires when <c>ProjectInto</c> is called more than once on the same query chain.
        /// </summary>
        public static readonly DiagnosticDescriptor DoubleProjectInto = new(
            id: DiagnosticIds.DoubleProjectInto,
            title: "ProjectInto called more than once in a query chain",
            messageFormat: "ProjectInto is called more than once on the same query chain. This throws InvalidOperationException at runtime.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB's ProjectInto can only be called once per query. Calling it a second time throws InvalidOperationException because the projection is already registered on the provider.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.DoubleProjectInto);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexMapAssignedOutsideCtor"/>.
        /// Fires when the <c>Map</c> or <c>Reduce</c> property of an index class is assigned outside a constructor.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexMapAssignedOutsideCtor = new(
            id: DiagnosticIds.IndexMapAssignedOutsideCtor,
            title: "Index Map or Reduce assigned outside constructor",
            messageFormat: "'{0}' is assigned outside a constructor. RavenDB index Map and Reduce expressions must be set in the constructor.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB reads the Map and Reduce expression trees during index registration, which happens when the constructor runs. Assigning them in a method that is called conditionally or after construction can result in the index being registered without a mapping.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.IndexMapAssignedOutsideCtor);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexMissingMapAssignment"/>.
        /// Fires when a class derived from <c>AbstractIndexCreationTask&lt;T&gt;</c> has no constructor that assigns <c>Map</c>.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexMissingMapAssignment = new(
            id: DiagnosticIds.IndexMissingMapAssignment,
            title: "AbstractIndexCreationTask subclass is missing a Map assignment",
            messageFormat: "Class '{0}' inherits from an index creation task but does not assign the Map property in its constructor",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "Every class that inherits from AbstractIndexCreationTask<TDocument> must assign the Map property in its constructor. Without a Map the index has no definition and will fail when deployed.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.IndexMissingMapAssignment);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.MultiMapIndexMissingAddMap"/>.
        /// Fires when a class derived from a multi-map index base has no constructor that calls <c>AddMap</c>.
        /// </summary>
        public static readonly DiagnosticDescriptor MultiMapIndexMissingAddMap = new(
            id: DiagnosticIds.MultiMapIndexMissingAddMap,
            title: "Multi-map index has no AddMap call in any constructor",
            messageFormat: "Class '{0}' inherits from a multi-map index creation task but does not call AddMap in its constructor",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "Every class that inherits from AbstractMultiMapIndexCreationTask (or the time-series / counters variants) must call AddMap<TSource>(...) at least once in its constructor. Without at least one AddMap the index has no definition and will fail when deployed.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.MultiMapIndexMissingAddMap);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.MultiMapIndexSingleAddMap"/>.
        /// Fires when a multi-map index class has exactly one <c>AddMap</c> call — a regular index would suffice.
        /// </summary>
        public static readonly DiagnosticDescriptor MultiMapIndexSingleAddMap = new(
            id: DiagnosticIds.MultiMapIndexSingleAddMap,
            title: "Multi-map index uses only a single AddMap",
            messageFormat: "Class '{0}' calls AddMap only once. Consider deriving from AbstractIndexCreationTask<T> instead of a multi-map base class.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Info,
            isEnabledByDefault: true,
            description: "Multi-map index base classes are designed for indexes that map over multiple document types. If only one AddMap call is present, a regular AbstractIndexCreationTask<TDocument> is simpler and equally expressive.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.MultiMapIndexSingleAddMap);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryFieldNotIndexed"/>.
        /// Fires when a Where/OrderBy/Search lambda references a field not in the index projection.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryFieldNotIndexed = new(
            id: DiagnosticIds.QueryFieldNotIndexed,
            title: "Query field not present in the index projection",
            messageFormat: "Field '{0}' is referenced in {1} but is not indexed by '{2}'. The query will not return results for this field.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Info,
            isEnabledByDefault: true,
            description: "When querying a specific index, all fields used in Where, OrderBy, and Search clauses should match the fields projected by the index Map expression. A field not in the projection cannot be searched or sorted efficiently.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.QueryFieldNotIndexed);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryProjectionFieldNotRetrievable"/>.
        /// Fires when a projected field is not retrievable under the applicable ProjectionBehavior.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryProjectionFieldNotRetrievable = new(
            id: DiagnosticIds.QueryProjectionFieldNotRetrievable,
            title: "Projected field not retrievable under the applied ProjectionBehavior",
            messageFormat: "Field '{0}' in projection is not retrievable from index '{1}' (not stored) and is not a member of source document '{2}'. The projected value will be missing at runtime under ProjectionBehavior.{3}.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Info,
            isEnabledByDefault: true,
            description: "RavenDB projections retrieve each field from the stored index entry or fall back to the source document. If a field is absent from both, the projected value will be null or missing at runtime. Under FromIndex or FromIndexOrThrow behaviors the fallback is disabled, making unstored fields silently empty or a hard error.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.QueryProjectionFieldNotRetrievable);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexUnsupportedMethodCall"/>.
        /// Fires when a user-defined method is called inside a Map, Reduce, or AddMap lambda.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexUnsupportedMethodCall = new(
            id: DiagnosticIds.IndexUnsupportedMethodCall,
            title: "Unsupported method call inside index Map/Reduce expression",
            messageFormat: "Method '{0}' is user-defined and may not be translatable inside an index {1} expression. Inline the operation or use a Raven-supported construct.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB compiles index Map and Reduce expressions to server-side IL. User-defined helper methods cannot be translated and will cause the index to fail at deployment or produce incorrect results. Inline the logic directly in the lambda or use only Raven-supported constructs (LINQ, BCL string/math, LoadDocument, etc.).",
            helpLinkUri: HelpLinkBase + DiagnosticIds.IndexUnsupportedMethodCall);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryUnsupportedMethodCall"/>.
        /// Fires when a user-defined method is called inside a lambda passed to a RavenDB query chain method.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryUnsupportedMethodCall = new(
            id: DiagnosticIds.QueryUnsupportedMethodCall,
            title: "Unsupported method call inside RavenDB query expression",
            messageFormat: "Method '{0}' is user-defined and may not translate to server-side query semantics. Move the call outside the query, inline the operation, or materialize with ToList() first.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB translates LINQ query lambdas (Where, OrderBy, Select, etc.) to RQL on the server. User-defined methods inside these lambdas cannot be translated and will throw at runtime. Compute the value before the query, inline the logic, or call ToList() first to evaluate client-side.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.QueryUnsupportedMethodCall);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.SubscriptionStoreOpenSession"/>.
        /// Fires when <c>OpenSession</c> or <c>OpenAsyncSession</c> is called on an
        /// <c>IDocumentStore</c> receiver inside a lambda passed to a subscription worker's
        /// <c>Run</c> method. The batch's own <c>OpenSession</c> / <c>OpenAsyncSession</c>
        /// must be used instead to participate in the batch's acknowledge transaction.
        /// </summary>
        public static readonly DiagnosticDescriptor SubscriptionStoreOpenSession = new(
            id: DiagnosticIds.SubscriptionStoreOpenSession,
            title: "Use batch.OpenSession inside a subscription Run delegate",
            messageFormat: "'{0}' is called on an IDocumentStore inside a subscription Run lambda. Use batch.{0}() instead to participate in the batch's acknowledge transaction.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "Inside a subscription worker Run delegate, sessions must be opened via the batch object (batch.OpenSession() / batch.OpenAsyncSession()), not via the document store. Using the store bypasses the batch's internal transaction tracking, which means the session will not participate in the batch acknowledgement and documents may be re-processed.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.SubscriptionStoreOpenSession);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.SessionLazyBatching"/>.
        /// Fires when a method contains 2+ independent materializing session operations
        /// (eager Load or materializing Query calls) that could be batched together
        /// using the lazy API.
        /// </summary>
        public static readonly DiagnosticDescriptor SessionLazyBatching = new(
            id: DiagnosticIds.SessionLazyBatching,
            title: "Batch independent session operations using the lazy API",
            messageFormat: "'{0}' is an eager session operation. This method contains multiple independent session operations; use session.Advanced.Lazily or query.Lazily() to batch them into a single server round-trip.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "Each eager Load or materializing Query (ToList, First, etc.) sends a separate HTTP request to the RavenDB server. When a method contains two or more independent operations, they can be registered as lazy and executed together in a single multi-get request, reducing latency. Use session.Advanced.Lazily.Load<T>() and query.Lazily() to register operations lazily, then access .Value or call session.Advanced.Eagerly.ExecuteAllPendingLazyOperations() to trigger the batch.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.SessionLazyBatching);

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryUnboundedResult"/>.
        /// Fires when a query materializes a full result set without an explicit .Take(n) call.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryUnboundedResult = new(
            id: DiagnosticIds.QueryUnboundedResult,
            title: "Query result is not bounded by Take()",
            messageFormat: "'{0}' returns an unbounded result set. Add .Take(n) before {0} to limit the number of documents fetched from the server.",
            category: Category,
            defaultSeverity: DiagnosticSeverity.Warning,
            isEnabledByDefault: true,
            description: "RavenDB queries default to returning at most 128 documents per request (the server's page size). Without an explicit .Take(n), the intent is invisible and the query may silently fetch far more data than intended as the dataset grows. Add .Take(n) to make the limit explicit.",
            helpLinkUri: HelpLinkBase + DiagnosticIds.QueryUnboundedResult);
    }
}
