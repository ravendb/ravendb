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
using Sparrow;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class SessionTimeSeriesBase
    {
        protected string DocId;
        protected string Name;
        protected InMemoryDocumentSessionOperations Session;

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, string documentId, string name)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));
            
            DocId = documentId;
            Name = name;
            Session = session;
        }

        protected SessionTimeSeriesBase(InMemoryDocumentSessionOperations session, object entity, string name)
        {
            if (session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false || document == null)
            {
                ThrowEntityNotInSession(entity);
                return;
            }

            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException(nameof(name));

            DocId = document.Id;
            Name = name;
            Session = session;
        }

        public void Append(DateTime timestamp, double value, string tag = null)
        {
            Append(timestamp, new []{ value }, tag);
        }

        public void Append(DateTime timestamp, IEnumerable<double> values, string tag = null)
        {
            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, Name);

            var op = new TimeSeriesOperation.AppendOperation
            {
                Timestamp = timestamp.EnsureUtc(),
                Tag = tag,
                Values = values.ToArray()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, Name), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;

                if (tsCmd.TimeSeries.Appends == null)
                    tsCmd.TimeSeries.Appends = new List<TimeSeriesOperation.AppendOperation>();

                tsCmd.TimeSeries.Appends.Add(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, Name, appends: new List<TimeSeriesOperation.AppendOperation> { op }, removals: null));
            }
        }

        public void Remove(DateTime at)
        {
            Remove(at, at);
        }

        public void Remove(DateTime from, DateTime to)
        {
            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, Name);

            var op = new TimeSeriesOperation.RemoveOperation
            {
                From = from.EnsureUtc(),
                To = to.EnsureUtc()
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.TimeSeries, Name), out var command))
            {
                var tsCmd = (TimeSeriesBatchCommandData)command;

                if (tsCmd.TimeSeries.Removals == null)
                    tsCmd.TimeSeries.Removals = new List<TimeSeriesOperation.RemoveOperation>();

                tsCmd.TimeSeries.Removals.Add(op);
            }
            else
            {
                Session.Defer(new TimeSeriesBatchCommandData(DocId, Name, appends: null, removals: new List<TimeSeriesOperation.RemoveOperation> { op }));
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
