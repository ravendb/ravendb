//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Documents.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Provides an access to DocumentSession TimeSeries API.
    /// </summary>
    public partial class DocumentSession
    {
        /// <inheritdoc/>
        public ISessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            ValidateTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        /// <inheritdoc/>
        public ISessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            ValidateTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        /// <inheritdoc/>
        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        /// <inheritdoc/>
        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }

        /// <inheritdoc/>
        public ISessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new SessionDocumentTimeSeries<TValues>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        /// <inheritdoc/>
        public ISessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new SessionDocumentTimeSeries<TValues>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        /// <inheritdoc/>
        public ISessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(string documentId, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        /// <inheritdoc/>
        public ISessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(object entity, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        /// <inheritdoc/>
        public ISessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        /// <inheritdoc/>
        public ISessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }
    }
}
