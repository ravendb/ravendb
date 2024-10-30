using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Extensions;
using Sparrow;

namespace Raven.Client.Documents.Session.Loaders
{
    /// <summary>
    /// The server is instructed to pre-load referenced documents concurrently with retrieving the documents.<br/>
    /// The documents are added to the session unit of work, and subsequent requests to load them are served directly from the session cache,
    /// without requiring any additional queries to the server. <br />
    /// The server can then be instructed to pre-load the referenced object at the same time that the root object is retrieved, for example using:
    /// <example>
    /// <code>
    /// Order order = session.Include&lt;Order&gt;(x => x.CustomerId).Load("orders/1-A");
    /// // this will not require querying the server:
    /// Customer customer = session.Load&lt;Customer&gt;(order.CustomerId);
    /// </code></example>
    /// </summary>
    /// <inheritdoc cref="DocumentationUrls.Session.Querying.Includes"/>
    public interface IDocumentIncludeBuilder<T, out TBuilder>
    {

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}"/>
        /// <param name="path">Name of property which contains id(s) of document(s) to include from queried document.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.Includes"/>
        TBuilder IncludeDocuments(string path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}"/>
        /// <param name="path">Path to the property which contains id of document to include.</param>
        TBuilder IncludeDocuments(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IDocumentIncludeBuilder{T,TBuilder}"/>
        /// <param name="path">Path to the property which contains ids of documents to include.</param>
        /// <inheritdoc cref="DocumentationUrls.Session.Querying.Includes"/>
        TBuilder IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path);

        /// <inheritdoc cref="IncludeDocuments(Expression{Func{T, string}})"/>
        /// <typeparam name="TInclude">Type of included document</typeparam>
        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IncludeDocuments(Expression{Func{T, IEnumerable{string}}})"/>
        /// <typeparam name="TInclude">Type of included document</typeparam>
        TBuilder IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path);
    }

    public interface ICounterIncludeBuilder<T, out TBuilder>
    {
        TBuilder IncludeCounter(string name);

        TBuilder IncludeCounters(string[] names);

        TBuilder IncludeAllCounters();
    }

    /// <summary>
    /// The server is instructed to pre-load referenced documents concurrently with retrieving the time series data.<br/>
    /// The documents are added to the session unit of work, and subsequent requests to load them are served directly from the session cache,
    /// without requiring any additional queries to the server.
    /// </summary>
    /// <remarks><inheritdoc cref="DocumentationUrls.Session.TimeSeries.Include"/><br/><br/>
    /// Note: <br/>
    /// If a document Id appears in the results multiple times, the document data will be fetched only once and cached. <br/>
    /// In order to allow for cache usage make sure NoTracking is set to 'false'. 
    /// </remarks>
    public interface ITimeSeriesIncludeBuilder
    {
        /// <inheritdoc cref="ITimeSeriesIncludeBuilder"/>
        ITimeSeriesIncludeBuilder IncludeTags();

        /// <inheritdoc cref="ITimeSeriesIncludeBuilder"/>
        ITimeSeriesIncludeBuilder IncludeDocument();
    }

    /// <summary>
    /// The server is instructed to include time series data when retrieving documents.<br/>
    /// The time series results are added to the session unit of work, and subsequent requests to load them are served directly from the session cache,
    /// without requiring any additional requests to the server.
    /// </summary>
    /// <remarks><inheritdoc cref="DocumentationUrls.Session.TimeSeries.IncludeWithQuery"/></remarks>
    public interface IAbstractTimeSeriesIncludeBuilder<T, out TBuilder>
    {
        /// <inheritdoc cref="IAbstractTimeSeriesIncludeBuilder{T, TBuilder}"/>
        /// <param name="name">The name of the time series to include.</param>
        /// <param name="type">Indicates how to retrieve the time series entries.
        /// When set to 'Last', retrieves entries from the end of the time series within the specified time range. <br/>
        /// <remarks>Note that <see cref="TimeSeriesRangeType"/> cannot be 'None' when time is specified.</remarks></param>
        /// <param name="time">The time range to consider when retrieving time series entries.</param>
        TBuilder IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time);

        /// <inheritdoc cref="IAbstractTimeSeriesIncludeBuilder{T, TBuilder}"/>
        /// <param name="name">The name of the time series to include.</param>
        /// <param name="type">Indicates how to retrieve the time series entries.
        /// When set to 'Last', retrieves the last X entries, where X is determined by the 'count' parameter. <br/>
        /// <remarks>Note that <see cref="TimeSeriesRangeType"/> cannot be 'None' when count is specified.</remarks></param>
        /// <param name="count">The maximum number of entries to take when retrieving time series entries.</param>
        TBuilder IncludeTimeSeries(string name, TimeSeriesRangeType type, int count);

        /// <inheritdoc cref="IncludeTimeSeries(string, TimeSeriesRangeType, TimeValue)"/>
        /// <param name="names">The names of the time series to include.</param>
        TBuilder IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time);

        /// <inheritdoc cref="IncludeTimeSeries(string, TimeSeriesRangeType, int)"/>
        /// <param name="names">The names of the time series to include.</param>
        TBuilder IncludeTimeSeries(string[] names, TimeSeriesRangeType type, int count);

        /// <inheritdoc cref="IncludeTimeSeries(string, TimeSeriesRangeType, TimeValue)"/> 
        TBuilder IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time);

        /// <inheritdoc cref="IncludeTimeSeries(string, TimeSeriesRangeType, int)"/> 
        TBuilder IncludeAllTimeSeries(TimeSeriesRangeType type, int count);
    }

    /// <summary>
    /// The server is instructed to include revisions of the specified documents when retrieving them.<br/>
    /// Once loaded into the session, revisions can be accessed without additional server requests, ensuring efficient 
    /// retrieval of document history.
    /// </summary>
    public interface IRevisionIncludeBuilder<T, out TBuilder> 
    {
        /// <summary>
        /// Include a single revision by specifying the path to the document property that contains the revision's change vector.
        /// Each time a document is modified, its change vector is updated. By storing the change vector in a dedicated 
        /// property, you can easily include revisions when the document is loaded.
        /// </summary>
        /// <param name="path">An expression indicating the property path containing the revision change vector.</param>
        TBuilder IncludeRevisions(Expression<Func<T, string>> path);

        /// <summary>
        /// Include multiple revisions by specifying the path to the document property that contains an array of change vectors.
        /// When modifications are made to the document, the updated change vectors can be stored, allowing you to easily
        /// include multiple revisions when the document is loaded.
        /// </summary>
        /// <param name="path">An expression indicating the property path containing the revision change vectors.</param>
        TBuilder IncludeRevisions(Expression<Func<T, IEnumerable<string>>> path);

        /// <summary>
        /// Include a single revision by specifying its creation time. 
        /// The specified time can be in local time or UTC; the server will convert it to UTC.
        /// If an exact match for the creation time is found, that revision will be included. 
        /// Otherwise, the first revision preceding the specified time will be returned.
        /// </summary>
        /// <param name="before">The creation time of the revision to include.</param>
        TBuilder IncludeRevisions(DateTime before);

    }
    
    public interface ITimeSeriesIncludeBuilder<T, out TBuilder> : IAbstractTimeSeriesIncludeBuilder<T, TBuilder>
    {
        /// <inheritdoc cref="IAbstractTimeSeriesIncludeBuilder{T, TBuilder}"/>
        /// <param name="name">The name of the time series to include.</param>
        /// <param name="from">The date and time from which to start including time series entries (inclusive). If not specified, the collection will start from the earliest possible date and time (DateTime.MinValue).</param>
        /// <param name="to">The date and time at which to stop including time series entries (inclusive). If not specified, the collection will continue until the latest possible date and time (DateTime.MaxValue).</param>
        TBuilder IncludeTimeSeries(string name, DateTime? from = null, DateTime? to = null);
    }

    /// <inheritdoc cref="IAbstractTimeSeriesIncludeBuilder{T, TBuilder}"/>
    public interface ISubscriptionTimeSeriesIncludeBuilder<T, out TBuilder> : IAbstractTimeSeriesIncludeBuilder<T, TBuilder>
    {
    }

    /// <summary>
    /// The server is instructed to include Compare Exchange values when retrieving documents.<br/>
    /// Compare Exchange items can be included both when loading entities and during query execution. 
    /// The session automatically tracks the included Compare Exchange items, allowing their values 
    /// to be accessed without making additional calls to the server.
    /// </summary>
    public interface ICompareExchangeValueIncludeBuilder<T, out TBuilder>
    {
        /// <summary>
        /// Include a single Compare Exchange value by specifying its key.
        /// The key should correspond to the Compare Exchange item you wish to include.
        /// </summary>
        /// <param name="path">The key of the Compare Exchange value to include.</param>
        TBuilder IncludeCompareExchangeValue(string path);

        /// <summary>
        /// Include a single Compare Exchange value by specifying a path to the key.
        /// This allows for dynamic resolution of the Compare Exchange key based on 
        /// the document's properties.
        /// </summary>
        /// <param name="path">An expression indicating the property path that resolves to the Compare Exchange key.</param>
        TBuilder IncludeCompareExchangeValue(Expression<Func<T, string>> path);

        /// <summary>
        /// Include multiple Compare Exchange values by specifying a path to a collection of keys.
        /// This allows for the inclusion of multiple Compare Exchange items in a single call.
        /// </summary>
        /// <param name="path">An expression indicating the property path that resolves to an array of Compare Exchange keys.</param>
        TBuilder IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path);
        
    }

    /// <summary>
    /// The server is instructed to include various types of related items when retrieving documents.<br/>
    /// The items are added to the session unit of work, and subsequent requests to load them are served directly from the session cache,
    /// without requiring any additional queries to the server.
    /// This interface combines functionalities for including documents, counters, time series, compare exchange values, and revisions.
    /// </summary>
    /// <typeparam name="T">The type of the document being built.</typeparam>
    /// <typeparam name="TBuilder">The type of the builder being used.</typeparam>
    public interface IIncludeBuilder<T, out TBuilder> : IDocumentIncludeBuilder<T, TBuilder>, ICounterIncludeBuilder<T, TBuilder>, ITimeSeriesIncludeBuilder<T, TBuilder>, ICompareExchangeValueIncludeBuilder<T, TBuilder>, IRevisionIncludeBuilder<T, TBuilder>
    {
    }

    /// <inheritdoc/>
    public interface IIncludeBuilder<T> : IIncludeBuilder<T, IIncludeBuilder<T>>
    {
    }

    public interface ISubscriptionIncludeBuilder<T> : IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>, ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>, ISubscriptionTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>
    {
    }

    public interface IQueryIncludeBuilder<T> : IIncludeBuilder<T, IQueryIncludeBuilder<T>>
    {
        /// <inheritdoc cref="IIncludeBuilder{T,TBuilder}"/>
        IQueryIncludeBuilder<T> IncludeCounter(Expression<Func<T, string>> path, string name);
        
        /// <inheritdoc cref="IIncludeBuilder{T,TBuilder}"/>
        IQueryIncludeBuilder<T> IncludeCounters(Expression<Func<T, string>> path, string[] names);

        /// <inheritdoc cref="IIncludeBuilder{T,TBuilder}"/>
        IQueryIncludeBuilder<T> IncludeAllCounters(Expression<Func<T, string>> path);

        /// <inheritdoc cref="IIncludeBuilder{T,TBuilder}"/>
        IQueryIncludeBuilder<T> IncludeTimeSeries(Expression<Func<T, string>> path, string name, DateTime from, DateTime to);
        
    }

    public class IncludeBuilder
    {
        internal HashSet<string> DocumentsToInclude;

        internal IEnumerable<AbstractTimeSeriesRange> TimeSeriesToInclude
        {
            get
            {
                if (TimeSeriesToIncludeBySourceAlias == null)
                    return null;

                TimeSeriesToIncludeBySourceAlias.TryGetValue(string.Empty, out var value);
                return value;
            }
        }

        internal string Alias;

        internal HashSet<string> CountersToInclude
        {
            get
            {
                if (CountersToIncludeBySourcePath == null)
                    return null;

                CountersToIncludeBySourcePath.TryGetValue(string.Empty, out var value);
                return value.CountersToInclude;
            }
        }

        internal bool AllCounters
        {
            get
            {
                if (CountersToIncludeBySourcePath == null)
                    return false;

                CountersToIncludeBySourcePath.TryGetValue(string.Empty, out var value);
                return value.AllCounters;
            }
        }

        internal Dictionary<string, (bool AllCounters, HashSet<string> CountersToInclude)> CountersToIncludeBySourcePath;

        internal Dictionary<string, HashSet<AbstractTimeSeriesRange>> TimeSeriesToIncludeBySourceAlias;
        internal HashSet<string> CompareExchangeValuesToInclude;
        internal HashSet<string> RevisionsToIncludeByChangeVector;
        internal DateTime? RevisionsToIncludeByDateTime;

        internal bool IncludeTimeSeriesTags;
        internal bool IncludeTimeSeriesDocument;
    }

    internal sealed class IncludeBuilder<T> : IncludeBuilder, IQueryIncludeBuilder<T>, IIncludeBuilder<T>, ISubscriptionIncludeBuilder<T>, ITimeSeriesIncludeBuilder
    {
        private readonly DocumentConventions _conventions;

        internal IncludeBuilder(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeCounter(Expression<Func<T, string>> path, string name)
        {
            IncludeCounterWithAlias(path, name);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeCounters(Expression<Func<T, string>> path, string[] names)
        {
            IncludeCountersWithAlias(path, names);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeAllCounters(Expression<Func<T, string>> path)
        {
            IncludeAllCountersWithAlias(path);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICounterIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeAllCounters()
        {
            IncludeAllCounters(string.Empty);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> ICounterIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeAllCounters()
        {
            IncludeAllCounters(string.Empty);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IDocumentIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IDocumentIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCounter(string name)
        {
            IncludeCounter(string.Empty, name);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCounters(string[] names)
        {
            IncludeCounters(string.Empty, names);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICounterIncludeBuilder<T, IIncludeBuilder<T>>.IncludeAllCounters()
        {
            IncludeAllCounters(string.Empty);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(string path)
        {
            IncludeDocuments(path);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, string>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, string>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IDocumentIncludeBuilder<T, IIncludeBuilder<T>>.IncludeDocuments<TInclude>(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeDocuments(IncludesUtil.GetPrefixedIncludePath<TInclude>(path.ToPropertyPath(_conventions), _conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ITimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string name, DateTime? from, DateTime? to)
        {
            IncludeTimeSeriesFromTo(string.Empty, name, from, to);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ITimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string name, DateTime? from, DateTime? to)
        {
            IncludeTimeSeriesFromTo(string.Empty, name, from, to);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IQueryIncludeBuilder<T>.IncludeTimeSeries(Expression<Func<T, string>> path, string name, DateTime from, DateTime to)
        {
            WithAlias(path);
            IncludeTimeSeriesFromTo(path.ToPropertyPath(_conventions), name, from, to);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(string path)
        {
            IncludeCompareExchangeValue(path);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, string>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(string path)
        {
            IncludeCompareExchangeValue(path);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, string>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> ICompareExchangeValueIncludeBuilder<T, IIncludeBuilder<T>>.IncludeCompareExchangeValue(Expression<Func<T, IEnumerable<string>>> path)
        {
            IncludeCompareExchangeValue(path.ToPropertyPath(_conventions));
            return this;
        }

        /// <inheritdoc />
        ITimeSeriesIncludeBuilder ITimeSeriesIncludeBuilder.IncludeTags()
        {
            IncludeTimeSeriesTags = true;
            return this;
        }

        /// <inheritdoc />
        ITimeSeriesIncludeBuilder ITimeSeriesIncludeBuilder.IncludeDocument()
        {
            IncludeTimeSeriesDocument = true;
            return this;
        }

        public ITimeSeriesIncludeBuilder IncludeTimeSeries(string name, DateTime? from = null, DateTime? to = null)
        {
            IncludeTimeSeriesFromTo(string.Empty, name, from, to);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, name, type, time);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, name, type, count);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndTime(names, type, time);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, int count)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndCount(names, type, count);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, Constants.TimeSeries.All, type, time);
            return this;
        }

        /// <inheritdoc />
        IIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, Constants.TimeSeries.All, type, count);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, name, type, time);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, name, type, count);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndTime(names, type, time);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, int count)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndCount(names, type, count);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, Constants.TimeSeries.All, type, time);
            return this;
        }

        /// <inheritdoc />
        IQueryIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, Constants.TimeSeries.All, type, count);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, name, type, time);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string name, TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, name, type, count);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, TimeValue time)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndTime(names, type, time);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeTimeSeries(string[] names, TimeSeriesRangeType type, int count)
        {
            IncludeArrayOfTimeSeriesByRangeTypeAndCount(names, type, count);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, TimeValue time)
        {
            IncludeTimeSeriesByRangeTypeAndTime(string.Empty, Constants.TimeSeries.All, type, time);
            return this;
        }

        /// <inheritdoc />
        ISubscriptionIncludeBuilder<T> IAbstractTimeSeriesIncludeBuilder<T, ISubscriptionIncludeBuilder<T>>.IncludeAllTimeSeries(TimeSeriesRangeType type, int count)
        {
            IncludeTimeSeriesByRangeTypeAndCount(string.Empty, Constants.TimeSeries.All, type, count);
            return this;
        }

        private void IncludeDocuments(string path)
        {
            if (DocumentsToInclude == null)
                DocumentsToInclude = new HashSet<string>();

            DocumentsToInclude.Add(path);
        }

        private void IncludeCompareExchangeValue(string path)
        {
            if (CompareExchangeValuesToInclude == null)
                CompareExchangeValuesToInclude = new HashSet<string>();

            CompareExchangeValuesToInclude.Add(path);
        }
        
        private void IncludeCounterWithAlias(Expression<Func<T, string>> path, string name)
        {
            WithAlias(path);
            IncludeCounter(path.ToPropertyPath(_conventions), name);
        }

        private void IncludeCountersWithAlias(Expression<Func<T, string>> path, string[] names)
        {
            WithAlias(path);
            IncludeCounters(path.ToPropertyPath(_conventions), names);
        }

        private void IncludeRevisionsBefore(DateTime dateTime)
        {
            RevisionsToIncludeByDateTime ??= new DateTime();
            RevisionsToIncludeByDateTime = dateTime.ToUniversalTime();
        }

        private void IncludeRevisionsByChangeVectors(string path)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentNullException(nameof(path));
            
            RevisionsToIncludeByChangeVector ??= new HashSet<string>();
            RevisionsToIncludeByChangeVector.Add(path);
        }

        private void IncludeCounter(string path, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            AssertNotAllAndAddNewEntryIfNeeded(path);

            CountersToIncludeBySourcePath[path]
                .CountersToInclude.Add(name);
        }

        private void IncludeCounters(string path, string[] names)
        {
            if (names == null)
                throw new ArgumentNullException(nameof(names));

            AssertNotAllAndAddNewEntryIfNeeded(path);

            foreach (var name in names)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new InvalidOperationException("Counters(string[] names) : 'names' should not " +
                                                        "contain null or whitespace elements");
                CountersToIncludeBySourcePath[path]
                    .CountersToInclude.Add(name);
            }
        }

        private void IncludeAllCountersWithAlias(Expression<Func<T, string>> path)
        {
            WithAlias(path);
            IncludeAllCounters(path.ToPropertyPath(_conventions));
        }

        private void IncludeAllCounters(string sourcePath)
        {
            if (CountersToIncludeBySourcePath == null)
            {
                CountersToIncludeBySourcePath = new Dictionary<string,
                    (bool, HashSet<string>)>(StringComparer.OrdinalIgnoreCase);
            }

            if (CountersToIncludeBySourcePath.TryGetValue(sourcePath, out var val) && val.CountersToInclude != null)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use AllCounters() after using Counter(string name) or Counters(string[] names)");

            CountersToIncludeBySourcePath[sourcePath] = (true, null);
        }

        private void AssertNotAllAndAddNewEntryIfNeeded(string path)
        {
            if (CountersToIncludeBySourcePath != null &&
                CountersToIncludeBySourcePath.TryGetValue(path, out var val) &&
                val.AllCounters)
                throw new InvalidOperationException("IIncludeBuilder : You cannot use Counter(name) after using AllCounters() ");

            if (CountersToIncludeBySourcePath == null)
            {
                CountersToIncludeBySourcePath = new Dictionary<string,
                    (bool, HashSet<string>)>(StringComparer.OrdinalIgnoreCase);
            }

            if (CountersToIncludeBySourcePath.ContainsKey(path) == false)
            {
                CountersToIncludeBySourcePath[path] = (false, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
            }
        }

        private void WithAlias(Expression<Func<T, string>> path)
        {
            if (Alias == null)
                Alias = path.Parameters[0].Name;
        }
        private void WithAlias(Expression<Func<T, IEnumerable<string>>> path)
        {
            Alias ??= path.Parameters[0].Name;
        }

        private void IncludeTimeSeriesFromTo(string alias, string name, DateTime? from, DateTime? to)
        {
            AssertValid(alias, name);

            if (TimeSeriesToIncludeBySourceAlias == null)
            {
                TimeSeriesToIncludeBySourceAlias = new Dictionary<string, HashSet<AbstractTimeSeriesRange>>();
            }

            if (TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet) == false)
            {
                TimeSeriesToIncludeBySourceAlias[alias] = hashSet = new HashSet<AbstractTimeSeriesRange>(comparer: AbstractTimeSeriesRangeComparer.Instance);
            }

            hashSet.Add(new TimeSeriesRange
            {
                Name = name,
                From = from?.EnsureUtc(),
                To = to?.EnsureUtc()
            });
        }

        private void IncludeTimeSeriesByRangeTypeAndTime(string alias, string name, TimeSeriesRangeType type, TimeValue time)
        {
            AssertValid(alias, name);
            AssertValidType(type, time);

            if (TimeSeriesToIncludeBySourceAlias == null)
            {
                TimeSeriesToIncludeBySourceAlias = new Dictionary<string, HashSet<AbstractTimeSeriesRange>>();
            }

            if (TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet) == false)
            {
                TimeSeriesToIncludeBySourceAlias[alias] = hashSet = new HashSet<AbstractTimeSeriesRange>(comparer: AbstractTimeSeriesRangeComparer.Instance);
            }

            hashSet.Add(new TimeSeriesTimeRange
            {
                Name = name,
                Time = time,
                Type = type
            });

            static void AssertValidType(TimeSeriesRangeType type, TimeValue time)
            {
                switch (type)
                {
                    case TimeSeriesRangeType.None:
                        throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.None)}' when time is specified.");
                    case TimeSeriesRangeType.Last:
                        if (time != default)
                        {
                            if (time.Value <= 0)
                                throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.Last)}' when time is negative or zero.");

                            return;
                        }

                        throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.Last)}' when time is not specified.");
                    default:
                        throw new NotSupportedException($"Not supported time range type '{type}'.");
                };
            }
        }

        private void IncludeTimeSeriesByRangeTypeAndCount(string alias, string name, TimeSeriesRangeType type, int count)
        {
            AssertValid(alias, name);
            AssertValidTypeAndCount(type, count);

            if (TimeSeriesToIncludeBySourceAlias == null)
            {
                TimeSeriesToIncludeBySourceAlias = new Dictionary<string, HashSet<AbstractTimeSeriesRange>>();
            }

            if (TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet) == false)
            {
                TimeSeriesToIncludeBySourceAlias[alias] = hashSet = new HashSet<AbstractTimeSeriesRange>(comparer: AbstractTimeSeriesRangeComparer.Instance);
            }

            hashSet.Add(new TimeSeriesCountRange
            {
                Name = name,
                Count = count,
                Type = type
            });

            static void AssertValidTypeAndCount(TimeSeriesRangeType type, int count)
            {
                switch (type)
                {
                    case TimeSeriesRangeType.None:
                        throw new InvalidOperationException($"Time range type cannot be set to '{nameof(TimeSeriesRangeType.None)}' when count is specified.");
                    case TimeSeriesRangeType.Last:
                        if (count <= 0)
                            throw new ArgumentException(nameof(count) + " have to be positive.");
                        break;
                    default:
                        throw new NotSupportedException($"Not supported time range type '{type}'.");
                };
            }
        }

        private void IncludeArrayOfTimeSeriesByRangeTypeAndTime(string[] names, TimeSeriesRangeType type, TimeValue time)
        {
            if (names is null)
                throw new ArgumentNullException(nameof(names));

            foreach (var name in names)
                IncludeTimeSeriesByRangeTypeAndTime(string.Empty, name, type, time);
        }

        private void IncludeArrayOfTimeSeriesByRangeTypeAndCount(string[] names, TimeSeriesRangeType type, int count)
        {
            if (names is null)
                throw new ArgumentNullException(nameof(names));

            foreach (var name in names)
                IncludeTimeSeriesByRangeTypeAndCount(string.Empty, name, type, count);
        }

        private void AssertValid(string alias, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            if (TimeSeriesToIncludeBySourceAlias != null && TimeSeriesToIncludeBySourceAlias.TryGetValue(alias, out var hashSet2) && hashSet2.Count > 0)
            {
                if (name == Constants.TimeSeries.All)
                    throw new InvalidOperationException("IIncludeBuilder : Cannot use 'IncludeAllTimeSeries' after using 'IncludeTimeSeries' or 'IncludeAllTimeSeries'.");

                if (hashSet2.Any(x => x.Name == Constants.TimeSeries.All))
                    throw new InvalidOperationException("IIncludeBuilder : Cannot use 'IncludeTimeSeries' or 'IncludeAllTimeSeries' after using 'IncludeAllTimeSeries'.");
            }
        }

        IQueryIncludeBuilder<T> IRevisionIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeRevisions(Expression<Func<T, string>> changeVectorPath)
        {
            WithAlias(changeVectorPath);
            IncludeRevisionsByChangeVectors(changeVectorPath.ToPropertyPath(_conventions));
            return this;
        }

        IIncludeBuilder<T> IRevisionIncludeBuilder<T, IIncludeBuilder<T>>.IncludeRevisions(Expression<Func<T, string>> changeVectorPath)
        {
            WithAlias(changeVectorPath);
            IncludeRevisionsByChangeVectors(changeVectorPath.ToPropertyPath(_conventions));
            return this;
        }

        IIncludeBuilder<T> IRevisionIncludeBuilder<T, IIncludeBuilder<T>>.IncludeRevisions(Expression<Func<T, IEnumerable<string>>> changeVectorPaths)
        {
            WithAlias(changeVectorPaths);
            IncludeRevisionsByChangeVectors(changeVectorPaths.ToPropertyPath(_conventions));
            return this;
        }
        
        IQueryIncludeBuilder<T> IRevisionIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeRevisions(DateTime before)
        {
            IncludeRevisionsBefore(before);
            return this;
        }     
        
        IIncludeBuilder<T> IRevisionIncludeBuilder<T, IIncludeBuilder<T>>.IncludeRevisions(DateTime before)
        {
            IncludeRevisionsBefore(before);
            return this;
            
        }       

        IQueryIncludeBuilder<T> IRevisionIncludeBuilder<T, IQueryIncludeBuilder<T>>.IncludeRevisions(Expression<Func<T, IEnumerable<string>>> changeVectorPaths)
        {
            WithAlias(changeVectorPaths);
            IncludeRevisionsByChangeVectors(changeVectorPaths.ToPropertyPath(_conventions));
            return this;
        }
    }

    internal sealed class AbstractTimeSeriesRangeComparer : IEqualityComparer<AbstractTimeSeriesRange>
    {
        public static AbstractTimeSeriesRangeComparer Instance = new AbstractTimeSeriesRangeComparer();

        private AbstractTimeSeriesRangeComparer()
        {
        }

        public bool Equals(AbstractTimeSeriesRange x, AbstractTimeSeriesRange y)
        {
            return string.Equals(x?.Name, y?.Name, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(AbstractTimeSeriesRange obj)
        {
            return obj.Name.ToLowerInvariant().GetHashCode();
        }
    }
}
