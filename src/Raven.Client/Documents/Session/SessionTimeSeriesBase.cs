//-----------------------------------------------------------------------
// <copyright file="SessionTimeSeriesBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.TimeSeries;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class SessionTimeSeriesBase
    {
        protected string DocId;
        protected InMemoryDocumentSessionOperations Session;

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            DocId = documentId;
            Session = session;

        }

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, object entity)
        {
            if (session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false || document == null)
            {
                ThrowEntityNotInSession(entity);
                return;
            }

            DocId = document.Id;
            Session = session;
        }

        public void Append(string timeseries, DateTime timestamp, string tag, IEnumerable<double> values)
        {
            if (string.IsNullOrWhiteSpace(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, timeseries);

            var op = new TimeSeriesOperation.AppendOperation
            {
                Name = timeseries,
                Timestamp = timestamp,
                Tag = tag,
                Values = values is double[] arr
                    ? arr
                    : values.ToArray()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, null), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;

                if (tsCmd.TimeSeries.Appends == null)
                    tsCmd.TimeSeries.Appends = new List<TimeSeriesOperation.AppendOperation>();

                tsCmd.TimeSeries.Appends.Add(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, appends: new List<TimeSeriesOperation.AppendOperation> { op }, removals: null));
            }
        }

        public void Remove(string timeseries, DateTime at)
        {
            Remove(timeseries, at, at);
        }

        public void Remove(string timeseries, DateTime from, DateTime to)
        {
            if (string.IsNullOrWhiteSpace(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, timeseries);

            var op = new TimeSeriesOperation.RemoveOperation
            {
                Name = timeseries,
                From = from,
                To = to
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, null), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;

                if (tsCmd.TimeSeries.Removals == null)
                    tsCmd.TimeSeries.Removals = new List<TimeSeriesOperation.RemoveOperation>();

                tsCmd.TimeSeries.Removals.Add(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, appends: null, removals: new List<TimeSeriesOperation.RemoveOperation> { op }));
            }
        }


        private static void ThrowDocumentAlreadyDeletedInSession(string documentId, string timeseries)
        {
            throw new InvalidOperationException($"Can't modify timeseries {timeseries} of document {documentId}, the document was already deleted in this session.");
        }


        protected void ThrowEntityNotInSession(object entity)
        {
            throw new ArgumentException("entity is not associated with the session, cannot add timeseries to it. " +
                                        "Use documentId instead or track the entity in the session.");
        }

    }
}
