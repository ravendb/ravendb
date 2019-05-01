//-----------------------------------------------------------------------
// <copyright file="SessionCountersBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
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

        public void Append(string timeseries, DateTime timestamp, string tag, double[] values)
        {
            if (string.IsNullOrWhiteSpace(timeseries))
                throw new ArgumentNullException(nameof(timeseries));

            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, timeseries);

            var op = new AppendTimeSeriesOperation
            {
                Name = timeseries,
                Timestamp = timestamp,
                Tag = tag,
                Values = values
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, null), out var command))
            {
                var tsCmd = (DocumentTimeSeriesOperation)command;

                if (tsCmd.Appends == null)
                    tsCmd.Appends = new List<AppendTimeSeriesOperation>();

                tsCmd.Appends.Add(op);
            }
            else
            {
                Session.Defer(new DocumentTimeSeriesOperation
                {
                    Id = DocId,
                    Appends = new List<AppendTimeSeriesOperation>
                    {
                        op
                    }
                });
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

            var op = new RemoveTimeSeriesOperation
            {
                Name = timeseries,
                From = from,
                To = to
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, null), out var command))
            {
                var tsCmd = (DocumentTimeSeriesOperation)command;

                if (tsCmd.Removals == null)
                    tsCmd.Removals = new List<RemoveTimeSeriesOperation>();

                tsCmd.Removals.Add(op);
            }
            else
            {
                Session.Defer(new DocumentTimeSeriesOperation
                {
                    Id = DocId,
                    Removals = new List<RemoveTimeSeriesOperation>
                    {
                        op
                    }
                });
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
