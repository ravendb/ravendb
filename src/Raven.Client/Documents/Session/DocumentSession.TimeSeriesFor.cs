//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
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
            ValidateTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public ISessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            ValidateTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public ISessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }

        public ISessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new SessionDocumentTimeSeries<TValues>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public ISessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new SessionDocumentTimeSeries<TValues>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public ISessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(string documentId, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public ISessionDocumentIncrementalTimeSeries IncrementalTimeSeriesFor(object entity, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new SessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public ISessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public ISessionDocumentTypedIncrementalTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new SessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }
    }
}
