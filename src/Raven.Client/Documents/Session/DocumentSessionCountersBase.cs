//-----------------------------------------------------------------------
// <copyright file="DocumentSessionCountersBase.cs" company="Hibernating Rhinos LTD">
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
    public abstract class DocumentSessionCountersBase 
    {
        protected string _docId;
        protected InMemoryDocumentSessionOperations _session;

        protected DocumentSessionCountersBase(InMemoryDocumentSessionOperations session, string documentId)
        {
            _session = session;

            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            _docId = documentId;
        }

        protected DocumentSessionCountersBase(InMemoryDocumentSessionOperations session, object entity)
        {
            _session = session;

            if (_session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);
            _docId = document?.Id;
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

            if (_session.DocumentsById.TryGetValue(_docId, out DocumentInfo documentInfo) &&
                _session.DeletedEntities.Contains(documentInfo.Entity))
                ThrowDocumentAlreadyDeletedInSession(_docId, counter);

            if (_session.DeferredCommandsDictionary.TryGetValue((_docId, CommandType.Counters, null), out var command))
            {
                var countersBatchCommandData = (CountersBatchCommandData)command;
                if (countersBatchCommandData.HasDelete(counter))
                {
                    ThrowIncrementCounterAfterDeleteAttempt(_docId, counter);
                }

                countersBatchCommandData.Counters.Operations.Add(counterOp);
            }
            else
            {
                _session.Defer(new CountersBatchCommandData(_docId, counterOp));
            }
        }

        public void Delete(string counter)
        {
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentNullException(nameof(counter));

            if (_session.DeferredCommandsDictionary.ContainsKey((_docId, CommandType.DELETE, null)))
                return; // no-op

            if (_session.DocumentsById.TryGetValue(_docId, out DocumentInfo documentInfo) &&
                _session.DeletedEntities.Contains(documentInfo.Entity))
                return; // no-op

            var counterOp = new CounterOperation
            {
                Type = CounterOperationType.Delete,
                CounterName = counter
            };

            if (_session.DeferredCommandsDictionary.TryGetValue((_docId, CommandType.Counters, null), out var command))
            {
                var countersBatchCommandData = (CountersBatchCommandData)command;
                if (countersBatchCommandData.HasIncrement(counter))
                {
                    ThrowDeleteCounterAfterIncrementAttempt(_docId, counter);
                }

                countersBatchCommandData.Counters.Operations.Add(counterOp);
            }
            else
            {
                _session.Defer(new CountersBatchCommandData(_docId, counterOp));
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
