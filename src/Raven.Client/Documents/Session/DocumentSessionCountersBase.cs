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
    public abstract class DocumentSessionCountersBase : AdvancedSessionExtentionBase
    {
        protected DocumentSessionCountersBase(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public void Increment(string documentId, string counter, long delta = 1)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentNullException(nameof(counter));
            var counterOp = new CounterOperation
            {
                Type = CounterOperationType.Increment,
                CounterName = counter,
                Delta = delta
            };

            if (DeferredCommandsDictionary.TryGetValue((documentId, CommandType.Counters, null), out var command))
            {
                ((CountersBatchCommandData)command).Counters.Operations.Add(counterOp);
            }
            else
            {
                Defer(new CountersBatchCommandData(documentId, counterOp));
            }
        }

        public void Increment(object entity, string counter, long delta = 1)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            Increment(document.Id, counter, delta);
        }

        public void Delete(string documentId, string counter)
        {
            if (string.IsNullOrWhiteSpace(documentId))
                throw new ArgumentNullException(nameof(documentId));
            if (string.IsNullOrWhiteSpace(counter))
                throw new ArgumentNullException(nameof(counter));

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.DELETE, null)))
                return; // no-op

            if (DocumentsById.TryGetValue(documentId, out DocumentInfo documentInfo) &&
                DeletedEntities.Contains(documentInfo.Entity))
                return; // no-op
/*

            if (DeferredCommandsDictionary.ContainsKey((documentId, CommandType.CounterOperation, null)))
                throw new InvalidOperationException($"Can't delete attachment {name} of document {documentId}, there is a deferred command registered to create an attachment with the same name.");
*/

            Defer(new CountersBatchCommandData(documentId, new CounterOperation
            {
                Type = CounterOperationType.Delete,
                CounterName = counter
            }));
        }


        public void Delete(object entity, string counter)
        {
            if (DocumentsByEntity.TryGetValue(entity, out DocumentInfo document) == false)
                ThrowEntityNotInSession(entity);

            Delete(document.Id, counter);
        }

        protected void ThrowEntityNotInSession(object entity)
        {
            throw new ArgumentException(entity + " is not associated with the session, cannot add counter to it. " +
                                        "Use documentId instead or track the entity in the session.", nameof(entity));
        }

    }
}
