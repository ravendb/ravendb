//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
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
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class AsyncDocumentSession
    {
        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(string documentId, string name)
        {
            ValidateTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public IAsyncSessionDocumentTimeSeries TimeSeriesFor(object entity, string name)
        {
            ValidateTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> TimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }

        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(object entity, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public IAsyncSessionDocumentRollupTypedTimeSeries<TValues> TimeSeriesRollupFor<TValues>(string documentId, string policy, string raw = null) where TValues : new()
        {
            var tsName = raw ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, $"{tsName}{TimeSeriesConfiguration.TimeSeriesRollupSeparator}{policy}");
        }

        public IAsyncSessionDocumentTimeSeries IncrementalTimeSeriesFor(string documentId, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, documentId, name);
        }

        public IAsyncSessionDocumentTimeSeries IncrementalTimeSeriesFor(object entity, string name)
        {
            ValidateIncrementalTimeSeriesName(name);

            return new AsyncSessionDocumentTimeSeries<TimeSeriesEntry>(this, entity, name);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(string documentId, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, documentId, tsName);
        }

        public IAsyncSessionDocumentTypedTimeSeries<TValues> IncrementalTimeSeriesFor<TValues>(object entity, string name = null) where TValues : new()
        {
            var tsName = name ?? TimeSeriesOperations.GetTimeSeriesName<TValues>(Conventions);
            ValidateIncrementalTimeSeriesName(tsName);

            return new AsyncSessionDocumentTimeSeries<TValues>(this, entity, tsName);
        }


        private const string IncrementalTimeSeriesPrefix = "INC:";

        private static void ValidateTimeSeriesName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new InvalidDataException("Time Series name must contain at least one character");

            if (name.StartsWith(IncrementalTimeSeriesPrefix, StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException($"Time Series name cannot start with {IncrementalTimeSeriesPrefix} prefix");
        }

        private static void ValidateIncrementalTimeSeriesName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new InvalidDataException("Incremental Time Series name must contain at least one character");

            if (name.StartsWith(IncrementalTimeSeriesPrefix, StringComparison.OrdinalIgnoreCase) == false)
                throw new InvalidDataException($"Incremental Time Series name must start with {IncrementalTimeSeriesPrefix} prefix");
        }
    }
}
