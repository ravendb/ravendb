namespace Raven.Analyzers
{
    /// <summary>
    /// Diagnostic rule identifiers for the RavenDB Roslyn analyzer package.
    /// </summary>
    public static class DiagnosticIds
    {
        /// <summary>
        /// Identifier for the "Map or Reduce assigned outside constructor" diagnostic.
        /// </summary>
        public const string IndexMapAssignedOutsideCtor = "RVN001";

        /// <summary>
        /// Identifier for the "filtering or ordering after projection" diagnostic.
        /// </summary>
        public const string QueryFilteringAfterProjection = "RVN002";

        /// <summary>
        /// Identifier for the "ProjectInto called more than once" diagnostic.
        /// </summary>
        public const string DoubleProjectInto = "RVN003";

        /// <summary>
        /// Identifier for the "AbstractIndexCreationTask subclass missing Map assignment" diagnostic.
        /// </summary>
        public const string IndexMissingMapAssignment = "RVN004";

        /// <summary>
        /// Identifier for the "AbstractMultiMap*IndexCreationTask subclass missing AddMap call" diagnostic.
        /// </summary>
        public const string MultiMapIndexMissingAddMap = "RVN005";

        /// <summary>
        /// Identifier for the "AbstractMultiMap*IndexCreationTask subclass with only one AddMap call" diagnostic.
        /// </summary>
        public const string MultiMapIndexSingleAddMap = "RVN006";

        /// <summary>
        /// Identifier for the "query field not present in the index projection" diagnostic.
        /// </summary>
        public const string QueryFieldNotIndexed = "RVN007";

        /// <summary>
        /// Identifier for the "projected field is not retrievable from index or source document" diagnostic.
        /// </summary>
        public const string QueryProjectionFieldNotRetrievable = "RVN008";

        /// <summary>
        /// Identifier for the "unsupported method call inside index Map/Reduce expression" diagnostic.
        /// </summary>
        public const string IndexUnsupportedMethodCall = "RVN009";

        /// <summary>
        /// Identifier for the "unsupported method call inside RavenDB query expression" diagnostic.
        /// </summary>
        public const string QueryUnsupportedMethodCall = "RVN010";

        /// <summary>
        /// Identifier for the "OpenSession/OpenAsyncSession called on IDocumentStore inside a
        /// subscription Run lambda" diagnostic.
        /// </summary>
        public const string SubscriptionStoreOpenSession = "RVN011";

        /// <summary>
        /// Identifier for the "multiple independent eager session operations could be batched
        /// using the lazy API to reduce server round-trips" diagnostic.
        /// </summary>
        public const string SessionLazyBatching = "RVN012";

        /// <summary>
        /// Identifier for the "query result is not bounded by Take()" diagnostic.
        /// </summary>
        public const string QueryUnboundedResult = "RVN013";
    }
}
