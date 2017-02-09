//-----------------------------------------------------------------------
// <copyright file="HiLoKeyGenerator.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using System.Threading;
#if !DNXCORE50
using System.Transactions;
#endif

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Connection;
using Raven.Client.Exceptions;
using Raven.Json.Linq;

namespace Raven.Client.Document
{
    /// <summary>
    /// Generate hilo numbers against a RavenDB document
    /// </summary>
    public class HiLoKeyGenerator : HiLoKeyGeneratorBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="HiLoKeyGenerator"/> class.
        /// </summary>
        public HiLoKeyGenerator(string tag, long capacity)
            : base(tag, capacity)
        {
        }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="convention">The convention.</param>
        /// <param name="entity">The entity.</param>
        /// <param name="databaseCommands">Low level database commands.</param>
        /// <returns></returns>
        public string GenerateDocumentKey(IDatabaseCommands databaseCommands, DocumentConvention convention, object entity)
        {
            return GetDocumentKeyFromId(convention, NextId(databaseCommands));
        }

        ///<summary>
        /// Create the next id (numeric)
        ///</summary>
        public long NextId(IDatabaseCommands commands)
        {
            while (true)
            {
                var myRange = Range; // thread safe copy

                var current = Interlocked.Increment(ref myRange.Current);
                if (current <= myRange.Max)
                    return current;

                if (interlockedLock.TryEnter() == false)
                {
                    Interlocked.Increment(ref threadsWaitingForRangeUpdate);
                    mre.WaitOne();
                    Interlocked.Decrement(ref threadsWaitingForRangeUpdate);
                    continue;
                }

                try
                {
                    mre.Reset();

                    if (Range != myRange)
                        // Lock was contended, and the max has already been changed.
                        continue;

                    Range = GetNextRange(commands);
                }
                finally
                {
                    mre.Set();
                    interlockedLock.Exit();
                }
            }
        }

        private RangeValue GetNextRange(IDatabaseCommands databaseCommands)
        {
#if !DNXCORE50
            using (new TransactionScope(TransactionScopeOption.Suppress))
            using (RavenTransactionAccessor.SupressExplicitRavenTransaction())
#endif
            using (databaseCommands.ForceReadFromMaster())
            using (databaseCommands.DisableAllCaching())
            {
                // we need the latest value of the capacity
                var calculatedCapacity = Interlocked.Read(ref capacity);
                ModifyCapacityIfRequired(ref calculatedCapacity);

                while (true)
                {
                    try
                    {
                        var minNextMax = Range.Max;
                        JsonDocument document;

                        try
                        {
                            document = GetDocument(databaseCommands);
                        }
                        catch (ConflictException e)
                        {
                            // resolving the conflict by selecting the highest number
                            var highestMax = e.ConflictedVersionIds
                                .Select(conflictedVersionId => GetMaxFromDocument(databaseCommands.Get(conflictedVersionId), minNextMax, calculatedCapacity))
                                .Max();

                            PutDocument(databaseCommands, new JsonDocument
                            {
                                Etag = e.Etag,
                                Metadata = new RavenJObject(),
                                DataAsJson = RavenJObject.FromObject(new { Max = highestMax }),
                                Key = HiLoDocumentKey
                            });

                            continue;
                        }

                        IncreaseCapacityIfRequired(ref calculatedCapacity);

                        long min, max;
                        if (document == null)
                        {
                            min = minNextMax + 1;
                            max = minNextMax + calculatedCapacity;
                            document = new JsonDocument
                            {
                                Etag = Etag.Empty,
                                // sending empty etag means - ensure the that the document does NOT exists
                                Metadata = new RavenJObject(),
                                DataAsJson = RavenJObject.FromObject(new { Max = max }),
                                Key = HiLoDocumentKey
                            };
                        }
                        else
                        {
                            var oldMax = GetMaxFromDocument(document, minNextMax, calculatedCapacity);
                            min = oldMax + 1;
                            max = oldMax + calculatedCapacity;

                            document.DataAsJson["Max"] = max;
                        }
                        PutDocument(databaseCommands, document);

                        return new RangeValue(min, max);
                    }
                    catch (ConcurrencyException)
                    {
                        // expected, we need to retry
                        // we'll try to increase the capacity
                        ModifyCapacityIfRequired(ref calculatedCapacity);
                    }
                }
            }
        }

        private void PutDocument(IDatabaseCommands databaseCommands, JsonDocument document)
        {
            databaseCommands.Put(HiLoDocumentKey, document.Etag,
                                 document.DataAsJson,
                                 document.Metadata);
        }

        private JsonDocument GetDocument(IDatabaseCommands databaseCommands)
        {
            var documents = databaseCommands.Get(new[] { HiLoDocumentKey, RavenKeyServerPrefix }, new string[0]);
            return HandleGetDocumentResult(documents);
        }
    }
}
