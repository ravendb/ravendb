//-----------------------------------------------------------------------
// <copyright file="AsyncSessionDocumentTimeSeries.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.TimeSeries;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AsyncSessionDocumentTimeSeries : SessionTimeSeriesBase, IAsyncSessionDocumentTimeSeries
    {
        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, string documentId) : base(session, documentId)
        {
        }

        public AsyncSessionDocumentTimeSeries(InMemoryDocumentSessionOperations session, object entity) : base(session, entity)
        {
        }
        public async Task<IEnumerable<TimeSeriesValue>> GetAsync(string timeseries, DateTime from, DateTime to, CancellationToken token = default)
        {
            if(Session.TimeSeriesByDocId.TryGetValue(DocId, out var cache))
            {
                if(cache.Values.TryGetValue(timeseries, out var series))
                {
                    //TODO: we need to properly handle this here, including merging multiple sections, getting just the values we need, etc.
                    return series.Values;
                }
            }

            if (Session.DocumentsById.TryGetValue(DocId, out var document) &&
                document.Metadata.TryGet(Constants.Documents.Metadata.TimeSeries, out BlittableJsonReaderArray metadataTimeSeries) &&
                metadataTimeSeries.BinarySearch(timeseries) < 0)
            {
                // the document is loaded in the session, but the metadata says that there is no such timeseries
                return Array.Empty<TimeSeriesValue>();
            }

            Session.IncrementRequestCount();

            var details = await Session.Operations.SendAsync(
                 new GetTimeSeriesOperation(DocId, timeseries, from, to), Session.SessionInfo, token: token)
                 .ConfigureAwait(false);
            
            //TODO: if value exists, merge new values

            if(Session.NoTracking == false)
            {
                if (Session.TimeSeriesByDocId.TryGetValue(DocId, out cache) == false)
                {
                    Session.TimeSeriesByDocId[DocId] = details;
                }
                else
                {
                    //TODO: if value exists, merge new values
                    Session.TimeSeriesByDocId[DocId] = details;
                }
            }
            //TODO: Make sure that we get just the relevant range
            return details.Values[timeseries].Values;

        }
    }
}
