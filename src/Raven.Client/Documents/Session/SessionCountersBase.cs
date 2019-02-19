//-----------------------------------------------------------------------
// <copyright file="SessionCountersBase.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.Counters;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract implementation for in memory session operations
    /// </summary>
    public abstract class SessionCountersBase 
    {
        protected string DocId;
        protected InMemoryDocumentSessionOperations Session;

        protected SessionCountersBase(InMemoryDocumentSessionOperations session, string documentId)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));

            DocId = documentId;
            Session = session;

        }

        protected SessionCountersBase(InMemoryDocumentSessionOperations session, object entity)
        {
            if (session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false || document == null)
            {
                ThrowEntityNotInSession(entity);
                return;
            }

            DocId = document.Id;
            Session = session;
        }

        public void Increment(string counter, long delta = 1)
        {
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentNullException(nameof(counter));
            var counterOp = new CounterOperation
            {
                Type = CounterOperationType.Increment,
                CounterName = counter,
                Delta = delta
            };

            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(DocId, counter);

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.Counters, null), out var command))
            {
                var countersBatchCommandData = (CountersBatchCommandData)command;
                if (countersBatchCommandData.HasDelete(counter))
                {
                    ThrowIncrementCounterAfterDeleteAttempt(DocId, counter);
                }

                countersBatchCommandData.Counters.Operations.Add(counterOp);
            }
            else
            {
                Session.Defer(new CountersBatchCommandData(DocId, counterOp));
            }
        }

        public void Delete(string counter)
        {
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentNullException(nameof(counter));

            if (Session.DeferredCommandsDictionary.ContainsKey((DocId, CommandType.DELETE, null)))
                return; // no-op

            if (Session.DocumentsById.TryGetValue(DocId, out DocumentInfo documentInfo) &&
                Session.DeletedEntities.Contains(documentInfo.Entity))
                return; // no-op

            var counterOp = new CounterOperation
            {
                Type = CounterOperationType.Delete,
                CounterName = counter
            };

            if (Session.DeferredCommandsDictionary.TryGetValue((DocId, CommandType.Counters, null), out var command))
            {
                var countersBatchCommandData = (CountersBatchCommandData)command;
                if (countersBatchCommandData.HasIncrement(counter))
                {
                    ThrowDeleteCounterAfterIncrementAttempt(DocId, counter);
                }

                countersBatchCommandData.Counters.Operations.Add(counterOp);
            }
            else
            {
                Session.Defer(new CountersBatchCommandData(DocId, counterOp));
            }

            if (Session.CountersByDocId.TryGetValue(DocId, out var cache))
            {
                cache.Values.Remove(counter);
            }

        }

        protected void ThrowEntityNotInSession(object entity)
        {
            throw new ArgumentException("entity is not associated with the session, cannot add counter to it. " +
                                        "Use documentId instead or track the entity in the session.");
        }

        private static void ThrowIncrementCounterAfterDeleteAttempt(string documentId, string counter)
        {
            throw new InvalidOperationException(
                $"Can't increment counter {counter} of document {documentId}, there is a deferred command registered to delete a counter with the same name.");
        }

        private static void ThrowDeleteCounterAfterIncrementAttempt(string documentId, string counter)
        {
            throw new InvalidOperationException(
                $"Can't delete counter {counter} of document {documentId}, there is a deferred command registered to increment a counter with the same name.");
        }

        private static void ThrowDocumentAlreadyDeletedInSession(string documentId, string counter)
        {
            throw new InvalidOperationException($"Can't increment counter {counter} of document {documentId}, the document was already deleted in this session.");
        }

    }
}
