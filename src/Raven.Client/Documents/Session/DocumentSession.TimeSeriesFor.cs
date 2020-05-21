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
        public ISessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public ISessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : TimeSeriesEntry
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>();
            return new SessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : TimeSeriesEntry
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>();
            return new SessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }
        
        public ISessionDocumentTypedTimeSeries<TimeSeriesRollupEntry<TValues>> RollupTimeSeriesFor<TValues>(string documentId, string policy, string name = null) where TValues : TimeSeriesEntry, new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>();
            return new SessionDocumentTimeSeries<TimeSeriesRollupEntry<TValues>>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public ISessionDocumentTypedTimeSeries<TimeSeriesRollupEntry<TValues>> RollupTimeSeriesFor<TValues>(object entity, string policy, string name = null) where TValues : TimeSeriesEntry, new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>();
            return new SessionDocumentTimeSeries<TimeSeriesRollupEntry<TValues>>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }
    }
}
