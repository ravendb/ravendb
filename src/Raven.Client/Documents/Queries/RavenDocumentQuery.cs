using System;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.Tokens;

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
        public static MethodCall Now() => Time.NowInstance;

        /// <summary>
        /// Returns the current UTC date and time on the server, adjusted by the specified offset and floor-rounded
        /// to the smallest precision unit. Translates to the <c>now(offset)</c> RQL function.
        /// </summary>
        /// <param name="offset">
        /// A duration string representing the time offset. The result is floor-rounded to the smallest unit specified.
        /// <para>
        /// Format: <c>[+|-]Ny[Nmo][Nd][Nh][Nm][Ns]</c> — units must appear in descending order (year to second).
        /// Not all units are required; only the ones you need. Spaces between components are allowed.
        /// </para>
        /// <para>
        /// Each unit supports aliases:
        /// <c>[+|-]N(y|year|years)[N(mo|month|months)][N(d|day|days)][N(h|hour|hours)][N(m|min|minute|minutes)][N(s|sec|second|seconds)]</c>
        /// </para>
        /// Examples: <c>"+1y6mo"</c>, <c>"-2hours30minutes"</c>, <c>"1 year 6 months"</c>, <c>"15d"</c> (defaults to positive).
        /// </param>
        /// <example>
        /// <code>
        /// session.Advanced.DocumentQuery&lt;Order&gt;()
        ///     .WhereGreaterThan(x => x.CreatedAt, RavenDocumentQuery.Now("-30d"));
        /// </code>
        /// </example>
        public static MethodCall Now(string offset) => new Time(WhereToken.MethodsType.Now, offset);

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
        public static MethodCall Today() => Time.TodayInstance;


        /// <summary>
        /// Retrieves a compare exchange value by key for use in a query filter. Translates to the <c>cmpxchg()</c> RQL function.
        /// </summary>
        /// <param name="key">The key of the compare exchange value.</param>
        /// <example>
        /// <code>
        /// session.Advanced.DocumentQuery&lt;User&gt;()
        ///     .WhereEquals(x => x.Name, RavenDocumentQuery.CmpXchg("active-user"));
        /// </code>
        /// </example>
        public static MethodCall CmpXchg(string key)
        {
            return new Session.CmpXchg
            {
                Args = new object[] { key }
            };
        }

        internal sealed class Time : MethodCall
        {
            internal static readonly Time NowInstance = new Time(WhereToken.MethodsType.Now);
            internal static readonly Time TodayInstance = new Time(WhereToken.MethodsType.Today);

            public WhereToken.MethodsType MethodType { get; }

            internal Time(WhereToken.MethodsType methodType)
            {
                MethodType = methodType;
                Args = Array.Empty<object>();
            }

            internal Time(WhereToken.MethodsType methodType, string offset)
            {
                MethodType = methodType;
                Args = new object[] { offset };
            }
        }
    }
}
