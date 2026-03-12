using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Provides static methods for use in DocumentQuery and AsyncDocumentQuery that are translated to RQL server-side operations.
    /// </summary>
    public static class RavenDocumentQuery
    {
        /// <summary>
        /// Returns the current UTC date and time on the server. Translates to the <c>now()</c> RQL function.
        /// For use with <c>WhereEquals</c>, <c>WhereGreaterThan</c>, and other DocumentQuery/AsyncDocumentQuery filter methods.
        /// </summary>
        /// <example>
        /// <code>
        /// session.Advanced.DocumentQuery&lt;Order&gt;()
        ///     .WhereGreaterThan(x => x.CreatedAt, RavenDocumentQuery.Now());
        /// </code>
        /// </example>
        public static Time Now() => Time.Now;

        /// <summary>
        /// Returns the start of the current UTC day (midnight) on the server. Translates to the <c>today()</c> RQL function.
        /// For use with <c>WhereEquals</c>, <c>WhereGreaterThan</c>, and other DocumentQuery/AsyncDocumentQuery filter methods.
        /// </summary>
        /// <example>
        /// <code>
        /// session.Advanced.DocumentQuery&lt;Order&gt;()
        ///     .WhereGreaterThanOrEqual(x => x.CreatedAt, RavenDocumentQuery.Today());
        /// </code>
        /// </example>
        public static Time Today() => Time.Today;
    }
}
