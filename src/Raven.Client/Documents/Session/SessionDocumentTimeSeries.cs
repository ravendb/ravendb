//-----------------------------------------------------------------------
// <copyright file="SessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Util;

namespace Raven.Client.Documents.Session
{
    public class SessionDocumentTimeSeries : ISessionDocumentTimeSeries
    {
        private readonly AsyncSessionDocumentTimeSeries _asyncSessionTimeSeries;

        public SessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, string documentId)
        {
            _asyncSessionTimeSeries = new AsyncSessionDocumentTimeSeries(session, documentId);
        }

        public SessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, object entity)
        {
            _asyncSessionTimeSeries = new AsyncSessionDocumentTimeSeries(session, entity);
        }

        public void Append(string timeseries, DateTime timestamp, string tag, double[] values)
        {
            _asyncSessionTimeSeries.Append(timeseries, timestamp, tag, values);
        }

        public IEnumerable<TimeSeriesValue> Get(string timeseries, DateTime from, DateTime to)
        {
            return AsyncHelpers.RunSync(() => _asyncSessionTimeSeries.GetAsync(timeseries, from, to));
        }

        public void Remove(string timeseries, DateTime from, DateTime to)
        {
            _asyncSessionTimeSeries.Remove(timeseries, from, to);
        }

        public void Remove(string timeseries, DateTime at)
        {
            _asyncSessionTimeSeries.Remove(timeseries, at);
        }
    }
}
