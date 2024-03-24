//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesFor(string, string)"/>
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            ValidateTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesFor(object, string)"/>
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            ValidateTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesFor{TValue}(string, string)"/>
        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesFor{TValue}(object, string)"/>
        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesRollupFor{TValue}(object, string, string)"/>
        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        /// <inheritdoc cref="IAsyncDocumentSession.TimeSeriesRollupFor{TValue}(string, string, string)"/>
        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        /// <inheritdoc cref="IAsyncDocumentSession.IncrementalTimeSeriesFor(string, string)"/>
        public IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(string documentId, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.IncrementalTimeSeriesFor(object, string)"/>
        public IAsyncSessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(object entity, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.IncrementalTimeSeriesFor{TValue}(string, string)"/>
        public IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        /// <inheritdoc cref="IAsyncDocumentSession.IncrementalTimeSeriesFor{TValue}(object, string)"/>
        public IAsyncSessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }
    }
}
