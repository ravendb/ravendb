using System.Collections.Generic;
using Microsoft.CodeAnalysis;

namespace Raven.Analyzers.Shared
{
    public static class DiagnosticDescriptors
    {
        private const string Category = "Usage";

        // Per-rule short links (ravendb.net/l/<code>) that redirect to the diagnostic's documentation
        // page. Using a redirector lets the doc target move without shipping a new analyzer.
        private const string HelpLinkBase = "https://ravendb.net/l/";

        // Declared before the descriptor fields so this initializer runs first: the Create factory
        // appends to it as each descriptor below is constructed.
        private static readonly List<DiagnosticSeverityPolicyEntry> _policies = new();

        /// <summary>
        /// The graduated-severity policy for every diagnostic, in declaration order. Each entry records
        /// the version the rule was introduced in, its destination severity, and the severity currently
        /// in effect for the built product version. Consumed by the severity-policy test.
        /// </summary>
        public static IReadOnlyList<DiagnosticSeverityPolicyEntry> Policies => _policies;

        /// <summary>
        /// Builds a descriptor whose <see cref="DiagnosticDescriptor.DefaultSeverity"/> is resolved from
        /// <paramref name="introducedAt"/> and <paramref name="destinationSeverity"/> against the
        /// product version this assembly was built against (Info in the introducing release, the
        /// destination severity afterwards), and records the policy for the test to assert against.
        /// </summary>
        private static DiagnosticDescriptor Create(
            string id,
            string title,
            string messageFormat,
            string introducedAt,
            DiagnosticSeverity destinationSeverity,
            string description,
            string helpCode)
        {
            DiagnosticDescriptor descriptor = new(
                id: id,
                title: title,
                messageFormat: messageFormat,
                category: Category,
                defaultSeverity: SeverityPolicy.Resolve(introducedAt, destinationSeverity),
                isEnabledByDefault: true,
                description: description,
                helpLinkUri: HelpLinkBase + helpCode);

            // The registry mirrors the descriptors: EffectiveSeverity IS the descriptor's shipped
            // DefaultSeverity, so the test asserts the real severity against the version policy.
            _policies.Add(new DiagnosticSeverityPolicyEntry(id, introducedAt, destinationSeverity, descriptor.DefaultSeverity));

            return descriptor;
        }

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryFilteringAfterProjection"/>.
        /// Fires when a filtering or ordering method appears after a projection in a RavenDB query chain.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryFilteringAfterProjection = Create(
            id: DiagnosticIds.QueryFilteringAfterProjection,
            title: "RavenDB query operator after projection",
            messageFormat: "'{0}' is called after a projection (.ProjectInto or .Select). Move the projection to the end of the query chain.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB query operators that bind against the source document or index shape (Where, OrderBy, Search, Spatial, OrderByDistance, OrderByScore, MoreLikeThis, VectorSearch, Filter, GroupBy, etc.) must appear before projection (ProjectInto, Select). Projection changes the element type; any operator placed after it operates on the projected shape rather than the source document, producing silent incorrect results or a runtime exception.",
            helpCode: "HXYWDE");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.DoubleProjectInto"/>.
        /// Fires when <c>ProjectInto</c> is called more than once on the same query chain.
        /// </summary>
        public static readonly DiagnosticDescriptor DoubleProjectInto = Create(
            id: DiagnosticIds.DoubleProjectInto,
            title: "ProjectInto called more than once in a query chain",
            messageFormat: "ProjectInto is called more than once on the same query chain. This throws InvalidOperationException at runtime.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB's ProjectInto can only be called once per query. Calling it a second time throws InvalidOperationException because the projection is already registered on the provider.",
            helpCode: "5EDKFL");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexMapAssignedOutsideCtor"/>.
        /// Fires when the <c>Map</c> or <c>Reduce</c> property of an index class is assigned outside a constructor.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexMapAssignedOutsideCtor = Create(
            id: DiagnosticIds.IndexMapAssignedOutsideCtor,
            title: "Index Map or Reduce assigned outside constructor",
            messageFormat: "'{0}' is assigned outside a constructor. RavenDB index Map and Reduce expressions must be set in the constructor.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB reads the Map and Reduce expression trees during index registration, which happens when the constructor runs. Assigning them in a method that is called conditionally or after construction can result in the index being registered without a mapping.",
            helpCode: "244GGC");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexMissingMapAssignment"/>.
        /// Fires when a class derived from <c>AbstractIndexCreationTask&lt;T&gt;</c> has no constructor that assigns <c>Map</c>.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexMissingMapAssignment = Create(
            id: DiagnosticIds.IndexMissingMapAssignment,
            title: "AbstractIndexCreationTask subclass is missing a Map assignment",
            messageFormat: "Class '{0}' inherits from an index creation task but does not assign the Map property in its constructor",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "Every class that inherits from AbstractIndexCreationTask<TDocument> must assign the Map property in its constructor. Without a Map the index has no definition and will fail when deployed.",
            helpCode: "DEIJZG");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.MultiMapIndexMissingAddMap"/>.
        /// Fires when a class derived from a multi-map index base has no constructor that calls <c>AddMap</c>.
        /// </summary>
        public static readonly DiagnosticDescriptor MultiMapIndexMissingAddMap = Create(
            id: DiagnosticIds.MultiMapIndexMissingAddMap,
            title: "Multi-map index has no AddMap call in any constructor",
            messageFormat: "Class '{0}' inherits from a multi-map index creation task but does not call AddMap in its constructor",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "Every class that inherits from AbstractMultiMapIndexCreationTask (or the time-series / counters variants) must call AddMap<TSource>(...) at least once in its constructor. Without at least one AddMap the index has no definition and will fail when deployed.",
            helpCode: "NKEYXB");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.MultiMapIndexSingleAddMap"/>.
        /// Fires when a multi-map index class has exactly one <c>AddMap</c> call — a regular index would suffice.
        /// This is a pure style suggestion and stays at Info severity.
        /// </summary>
        public static readonly DiagnosticDescriptor MultiMapIndexSingleAddMap = Create(
            id: DiagnosticIds.MultiMapIndexSingleAddMap,
            title: "Multi-map index uses only a single AddMap",
            messageFormat: "Class '{0}' calls AddMap only once. Consider deriving from AbstractIndexCreationTask<T> instead of a multi-map base class.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Info,
            description: "Multi-map index base classes are designed for indexes that map over multiple document types. If only one AddMap call is present, a regular AbstractIndexCreationTask<TDocument> is simpler and equally expressive.",
            helpCode: "KZTWYV");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryFieldNotIndexed"/>.
        /// Fires when a Where/OrderBy/Search lambda references a field not in the index projection.
        /// Heuristic advisory; stays at Info severity.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryFieldNotIndexed = Create(
            id: DiagnosticIds.QueryFieldNotIndexed,
            title: "Query field not present in the index projection",
            messageFormat: "Field '{0}' is referenced in {1} but is not indexed by '{2}'. The query will not return results for this field.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Info,
            description: "When querying a specific index, all fields used in Where, OrderBy, and Search clauses should match the fields projected by the index Map expression. A field not in the projection cannot be searched or sorted efficiently.",
            helpCode: "148URW");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryProjectionFieldNotRetrievable"/>.
        /// Fires when a projected field is not retrievable under the applicable ProjectionBehavior.
        /// Heuristic advisory; stays at Info severity.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryProjectionFieldNotRetrievable = Create(
            id: DiagnosticIds.QueryProjectionFieldNotRetrievable,
            title: "Projected field not retrievable under the applied ProjectionBehavior",
            messageFormat: "Field '{0}' in projection is not retrievable from index '{1}' (not stored) and is not a member of source document '{2}'. The projected value will be missing at runtime under ProjectionBehavior.{3}.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Info,
            description: "RavenDB projections retrieve each field from the stored index entry or fall back to the source document. If a field is absent from both, the projected value will be null or missing at runtime. Under FromIndex or FromIndexOrThrow behaviors the fallback is disabled, making unstored fields silently empty or a hard error.",
            helpCode: "LPJ9EL");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexUnsupportedMethodCall"/>.
        /// Fires when a user-defined method is called inside a Map, Reduce, or AddMap lambda.
        /// </summary>
        public static readonly DiagnosticDescriptor IndexUnsupportedMethodCall = Create(
            id: DiagnosticIds.IndexUnsupportedMethodCall,
            title: "Unsupported method call inside index Map/Reduce expression",
            messageFormat: "Method '{0}' is user-defined and may not be translatable inside an index {1} expression. Inline the operation or use a Raven-supported construct.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB compiles index Map and Reduce expressions to server-side IL. User-defined helper methods cannot be translated and will cause the index to fail at deployment or produce incorrect results. Inline the logic directly in the lambda or use only Raven-supported constructs (LINQ, BCL string/math, LoadDocument, etc.).",
            helpCode: "PGOSBH");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryUnsupportedMethodCall"/>.
        /// Fires when a user-defined method is called inside a lambda passed to a RavenDB query chain method.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryUnsupportedMethodCall = Create(
            id: DiagnosticIds.QueryUnsupportedMethodCall,
            title: "Unsupported method call inside RavenDB query expression",
            messageFormat: "Method '{0}' is user-defined and may not translate to server-side query semantics. Move the call outside the query, inline the operation, or materialize with ToList() first.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB translates LINQ query lambdas (Where, OrderBy, Select, etc.) to RQL on the server. User-defined methods inside these lambdas cannot be translated and will throw at runtime. Compute the value before the query, inline the logic, or call ToList() first to evaluate client-side.",
            helpCode: "5X6TTO");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.SubscriptionStoreOpenSession"/>.
        /// Fires when <c>OpenSession</c> or <c>OpenAsyncSession</c> is called on an
        /// <c>IDocumentStore</c> receiver inside a lambda passed to a subscription worker's
        /// <c>Run</c> method. The batch's own <c>OpenSession</c> / <c>OpenAsyncSession</c>
        /// must be used instead to participate in the batch's acknowledge transaction.
        /// </summary>
        public static readonly DiagnosticDescriptor SubscriptionStoreOpenSession = Create(
            id: DiagnosticIds.SubscriptionStoreOpenSession,
            title: "Use batch.OpenSession inside a subscription Run delegate",
            messageFormat: "'{0}' is called on an IDocumentStore inside a subscription Run lambda. Use batch.{0}() instead to participate in the batch's acknowledge transaction.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "Inside a subscription worker Run delegate, sessions must be opened via the batch object (batch.OpenSession() / batch.OpenAsyncSession()), not via the document store. Using the store bypasses the batch's internal transaction tracking, which means the session will not participate in the batch acknowledgement and documents may be re-processed.",
            helpCode: "P13F9F");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.SessionLazyBatching"/>.
        /// Fires when a method contains 2+ independent materializing session operations
        /// (eager Load or materializing Query calls) that could be batched together
        /// using the lazy API.
        /// </summary>
        public static readonly DiagnosticDescriptor SessionLazyBatching = Create(
            id: DiagnosticIds.SessionLazyBatching,
            title: "Batch independent session operations using the lazy API",
            messageFormat: "'{0}' is an eager session operation. This method contains multiple independent session operations; use session.Advanced.Lazily or query.Lazily() to batch them into a single server round-trip.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "Each eager Load or materializing Query (ToList, First, etc.) sends a separate HTTP request to the RavenDB server. When a method contains two or more independent operations, they can be registered as lazy and executed together in a single multi-get request, reducing latency. Use session.Advanced.Lazily.Load<T>() and query.Lazily() to register operations lazily, then access .Value or call session.Advanced.Eagerly.ExecuteAllPendingLazyOperations() to trigger the batch.",
            helpCode: "N74R8M");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.QueryUnboundedResult"/>.
        /// Fires when a query materializes a full result set without an explicit .Take(n) call.
        /// </summary>
        public static readonly DiagnosticDescriptor QueryUnboundedResult = Create(
            id: DiagnosticIds.QueryUnboundedResult,
            title: "Query result is not bounded by Take()",
            messageFormat: "'{0}' returns an unbounded result set. Add .Take(n) before {0} to limit the number of documents fetched from the server.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "RavenDB queries default to returning at most 128 documents per request (the server's page size). Without an explicit .Take(n), the intent is invisible and the query may silently fetch far more data than intended as the dataset grows. Add .Take(n) to make the limit explicit.",
            helpCode: "CCHF6Z");

        /// <summary>
        /// Descriptor for <see cref="DiagnosticIds.IndexFanOut"/>.
        /// Fires when an index Map or AddMap lambda contains a fan-out operation (SelectMany or nested from clause).
        /// </summary>
        public static readonly DiagnosticDescriptor IndexFanOut = Create(
            id: DiagnosticIds.IndexFanOut,
            title: "Index Map fans out over a collection",
            messageFormat: "Index Map fans out via '{0}'. Each source document yields one index entry per element in the collection; unbounded collections can significantly degrade indexing performance.",
            introducedAt: "7.2",
            destinationSeverity: DiagnosticSeverity.Warning,
            description: "Fan-out indexes produce multiple index entries per source document by iterating over a nested collection. The RavenDB server fires a runtime warning (WarnIndexOutputsPerDocument) for the same reason. Verify the collection is intentionally fanned out and that its cardinality is acceptable.",
            helpCode: "9S4SVZ");
    }
}
