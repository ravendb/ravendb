using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries
{
    /// <summary>
    /// Provides static methods for use in LINQ queries that are translated to RavenDB server-side operations.
    /// These methods are designed for strongly-typed support in LINQ queries and throw exceptions when called directly in client code.
    /// </summary>
    public sealed class RavenQuery
    {
        /// <summary>
        /// Loads a document by its identifier. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the document to load.</typeparam>
        /// <param name="id">The document identifier.</param>
        /// <returns>The loaded document.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static T Load<T>(string id)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets the identifier of a document instance. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="documentInstance">The document instance.</param>
        /// <returns>The document identifier.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static string Id(object documentInstance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Loads multiple documents by their identifiers. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the documents to load.</typeparam>
        /// <param name="ids">The collection of document identifiers.</param>
        /// <returns>The loaded documents.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static IEnumerable<T> Load<T>(IEnumerable<string> ids)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Executes raw JavaScript code in the query. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The return type of the JavaScript execution.</typeparam>
        /// <param name="js">The JavaScript code to execute.</param>
        /// <returns>The result of the JavaScript execution.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static T Raw<T>(string js)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Executes raw JavaScript code in the query with a path context. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The return type of the JavaScript execution.</typeparam>
        /// <param name="path">The path context for the JavaScript execution.</param>
        /// <param name="js">The JavaScript code to execute.</param>
        /// <returns>The result of the JavaScript execution.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static T Raw<T>(T path, string js)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets the last modified date of a document instance. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the document instance.</typeparam>
        /// <param name="instance">The document instance.</param>
        /// <returns>The last modified date of the document.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static DateTime LastModified<T>(T instance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets the metadata of a document instance. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the document instance.</typeparam>
        /// <param name="instance">The document instance.</param>
        /// <returns>The metadata dictionary of the document.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static IMetadataDictionary Metadata<T>(T instance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a compare exchange value by key. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the compare exchange value.</typeparam>
        /// <param name="key">The compare exchange key.</param>
        /// <returns>The compare exchange value.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static T CmpXchg<T>(string key)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a counter value by name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="name">The counter name.</param>
        /// <returns>The counter value.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static long? Counter(string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a counter value by document identifier and counter name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="docId">The document identifier.</param>
        /// <param name="name">The counter name.</param>
        /// <returns>The counter value.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static long? Counter(string docId, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a counter value by document instance and counter name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="documentInstance">The document instance.</param>
        /// <param name="name">The counter name.</param>
        /// <returns>The counter value.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static long? Counter(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a time series by name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="name">The time series name.</param>
        /// <returns>A queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable TimeSeries(string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a time series by document instance and name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="documentInstance">The document instance.</param>
        /// <param name="name">The time series name.</param>
        /// <returns>A queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable TimeSeries(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a time series by document instance, name, and time range. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <param name="documentInstance">The document instance.</param>
        /// <param name="name">The time series name.</param>
        /// <param name="from">The start time of the range.</param>
        /// <param name="to">The end time of the range.</param>
        /// <returns>A queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable TimeSeries(object documentInstance, string name, DateTime from, DateTime to)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a strongly-typed time series by name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the time series entries.</typeparam>
        /// <param name="name">The time series name.</param>
        /// <returns>A strongly-typed queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable<T> TimeSeries<T>(string name) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a strongly-typed time series by document instance and name. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the time series entries.</typeparam>
        /// <param name="documentInstance">The document instance.</param>
        /// <param name="name">The time series name.</param>
        /// <returns>A strongly-typed queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable<T> TimeSeries<T>(object documentInstance, string name) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Gets a strongly-typed time series by document instance, name, and time range. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the time series entries.</typeparam>
        /// <param name="documentInstance">The document instance.</param>
        /// <param name="name">The time series name.</param>
        /// <param name="from">The start time of the range.</param>
        /// <param name="to">The end time of the range.</param>
        /// <returns>A strongly-typed queryable time series interface.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static ITimeSeriesQueryable<T> TimeSeries<T>(object documentInstance, string name, DateTime from, DateTime to) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Returns the current UTC date and time on the server. Translates to the <c>now()</c> RQL function.
        /// For use in LINQ queries only. For DocumentQuery and AsyncDocumentQuery, use <see cref="RavenDocumentQuery.Now"/> instead.
        /// </summary>
        /// <returns>The current UTC date and time.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static DateTime Now()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Returns the current UTC date and time on the server, adjusted by the specified offset and floor-rounded
        /// to the smallest precision unit. Translates to the <c>now(offset)</c> RQL function.
        /// For use in LINQ queries only. For DocumentQuery and AsyncDocumentQuery, use <see cref="RavenDocumentQuery.Now(string)"/> instead.
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
        /// <returns>The adjusted and rounded UTC date and time.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static DateTime Now(string offset)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        /// <summary>
        /// Returns the start of the current UTC day (midnight) on the server. Translates to the <c>today()</c> RQL function.
        /// For use in LINQ queries only. For DocumentQuery and AsyncDocumentQuery, use <see cref="RavenDocumentQuery.Today"/> instead.
        /// </summary>
        /// <returns>The start of the current UTC day.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static DateTime Today()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }


        /// <summary>
        /// Includes a related document path in the query for loading. This method is for server-side LINQ query translation only.
        /// </summary>
        /// <typeparam name="T">The type of the document.</typeparam>
        /// <param name="path">Expression specifying the path to include.</param>
        /// <returns>An object representing the include instruction.</returns>
        /// <exception cref="NotSupportedException">Thrown when called directly in client code.</exception>
        public static object Include<T>(Expression<Func<T, string>> path)
        {
            throw new NotSupportedException(
                "This method is here for strongly type support of server-side calls during LINQ queries and should never be directly called.");
        }
    }
}
