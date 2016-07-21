// -----------------------------------------------------------------------
//  <copyright file="SchemaCreator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Threading;

using Raven.Abstractions.Logging;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Storage.Voron.Impl;
using Raven.Database.Util;

using Voron;
using Voron.Impl;

namespace Raven.Database.Storage.Voron.Schema
{
    internal class SchemaCreator
    {
        internal readonly TableStorage storage;

        private readonly Action<string> output;

        private readonly ILog log;

        public const string SchemaVersion = "1.4";

        [ImportMany]
        public OrderedPartCollection<ISchemaUpdate> Updaters { get; set; }

        private static readonly object UpdateLocker = new object();

        public SchemaCreator(InMemoryRavenConfiguration configuration, TableStorage storage, Action<string> output, ILog log)
        {
            this.storage = storage;
            this.output = output;
            this.log = log;

            configuration.Container.SatisfyImportsOnce(this);
        }

        //create all relevant storage trees in one place
        public void CreateSchema()
        {
            using (var tx = storage.Environment.NewTransaction(TransactionFlags.ReadWrite))
            {
                CreateDetailsSchema(tx, storage);
                CreateDocumentsSchema(tx, storage);
                CreateIndexingStatsSchema(tx, storage);
                CreateLastIndexedEtagsSchema(tx, storage);
                CreateDocumentReferencesSchema(tx, storage);
                CreateQueuesSchema(tx, storage);
                CreateListsSchema(tx, storage);
                CreateTasksSchema(tx, storage);
                CreateStalenessSchema(tx, storage);
                CreateScheduledReductionsSchema(tx, storage);
                CreateMappedResultsSchema(tx, storage);
                CreateAttachmentsSchema(tx, storage);
                CreateReduceKeyCountsSchema(tx, storage);
                CreateReduceKeyTypesSchema(tx, storage);
                CreateReduceResultsSchema(tx, storage);
                CreateGeneralSchema(tx, storage);
                CreateReduceStatsSchema(tx, storage);
                CreateIndexingMetadataSchema(tx, storage);

                tx.Commit();
            }
        }

        public void SetupDatabaseIdAndSchemaVersion()
        {
            using (var snapshot = storage.CreateSnapshot())
            {
                var idSlice = new Slice("id");
                var schemaVersionSlice = new Slice("schema_version");

                Guid id;
                string schemaVersion;

                var read = storage.Details.Read(snapshot, idSlice, null);
                if (read == null) // new db
                {
                    id = Guid.NewGuid();
                    schemaVersion = SchemaVersion;
                    using (var writeIdBatch = new WriteBatch())
                    {
                        storage.Details.Add(writeIdBatch, idSlice, id.ToByteArray());
                        storage.Details.Add(writeIdBatch, schemaVersionSlice, schemaVersion);
                        storage.Write(writeIdBatch);
                    }
                }
                else
                {
                    if (read.Reader.Length != 16) //precaution - might prevent NRE in edge cases
                        throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption. (no db id)");

                    using (var stream = read.Reader.AsStream())
                    using (var reader = new BinaryReader(stream))
                    {
                        id = new Guid(reader.ReadBytes((int)stream.Length));
                    }

                    var schemaRead = storage.Details.Read(snapshot, schemaVersionSlice, null);
                    if (schemaRead == null)
                        throw new InvalidDataException("Failed to initialize Voron transactional storage. Possible data corruption. (no schema version)");

                    schemaVersion = schemaRead.Reader.ToStringValue();
                }

                storage.SetDatabaseIdAndSchemaVersion(id, schemaVersion);
            }
        }

        public void UpdateSchemaIfNecessary()
        {
            if (storage.SchemaVersion == SchemaVersion)
                return;

            using (var ticker = new OutputTicker(TimeSpan.FromSeconds(3), () =>
            {
                log.Info(".");
                Console.Write(".");
            }, null, () =>
            {
                log.Info("OK");
                Console.Write("OK");
                Console.WriteLine();
            }))
            {
                bool lockTaken = false;
                try
                {
                    Monitor.TryEnter(UpdateLocker, TimeSpan.FromSeconds(15), ref lockTaken);
                    if (lockTaken == false)
                        throw new TimeoutException("Could not take upgrade lock after 15 seconds, probably another database is upgrading itself and we can't interupt it midway. Please try again later");

                    do
                    {
                        var updater = Updaters.FirstOrDefault(update => update.Value.FromSchemaVersion == storage.SchemaVersion);
                        if (updater == null)
                            throw new InvalidOperationException(
                                string.Format(
                                    "The version on disk ({0}) is different that the version supported by this library: {1}{2}You need to migrate the disk version to the library version, alternatively, if the data isn't important, you can delete the file and it will be re-created (with no data) with the library version.",
                                    storage.SchemaVersion, SchemaVersion, Environment.NewLine));

                        log.Info("Updating schema from version {0}: ", storage.SchemaVersion);
                        Console.WriteLine("Updating schema from version {0}: ", storage.SchemaVersion);

                        ticker.Start();

                        updater.Value.Update(storage, output);
                        updater.Value.UpdateSchemaVersion(storage, output);

                        ticker.Stop();

                    } while (storage.SchemaVersion != SchemaVersion);
                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(UpdateLocker);
                }
            }
        }

        internal static void CreateIndexingMetadataSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.IndexingMetadata.TableName);
        }

        internal static void CreateReduceStatsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.ReduceStats.TableName);
        }

        internal static void CreateReduceResultsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.ReduceResults.TableName);
            storage.Environment.CreateTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevel));
            storage.Environment.CreateTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndSourceBucket));
            storage.Environment.CreateTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByViewAndReduceKeyAndLevelAndBucket));
            storage.Environment.CreateTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.ByView));
            storage.Environment.CreateTree(tx, storage.ReduceResults.GetIndexKey(Tables.ReduceResults.Indices.Data));
        }

        internal static void CreateReduceKeyCountsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.ReduceKeyCounts.TableName);
            storage.Environment.CreateTree(tx, storage.ReduceKeyCounts.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
        }

        internal static void CreateReduceKeyTypesSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.ReduceKeyTypes.TableName);
            storage.Environment.CreateTree(tx, storage.ReduceKeyTypes.GetIndexKey(Tables.ReduceKeyCounts.Indices.ByView));
        }

        [Obsolete("Use RavenFS instead.")]
        private static void CreateAttachmentsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Attachments.TableName);
            storage.Environment.CreateTree(tx, storage.Attachments.GetIndexKey(Tables.Attachments.Indices.ByEtag));
            storage.Environment.CreateTree(tx, storage.Attachments.GetIndexKey(Tables.Attachments.Indices.Metadata));
        }

        internal static void CreateMappedResultsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.MappedResults.TableName);
            storage.Environment.CreateTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByView));
            storage.Environment.CreateTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndDocumentId));
            storage.Environment.CreateTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKey));
            storage.Environment.CreateTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.ByViewAndReduceKeyAndSourceBucket));
            storage.Environment.CreateTree(tx, storage.MappedResults.GetIndexKey(Tables.MappedResults.Indices.Data));
        }

        internal static void CreateScheduledReductionsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.ScheduledReductions.TableName);
            storage.Environment.CreateTree(tx, storage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByView));
            storage.Environment.CreateTree(tx, storage.ScheduledReductions.GetIndexKey(Tables.ScheduledReductions.Indices.ByViewAndLevelAndReduceKey));
        }

        internal static void CreateStalenessSchema(Transaction tx, TableStorage storage)
        {
        }

        internal static void CreateTasksSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Tasks.TableName);
            storage.Environment.CreateTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndexAndType));
            storage.Environment.CreateTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByType));
            storage.Environment.CreateTree(tx, storage.Tasks.GetIndexKey(Tables.Tasks.Indices.ByIndex));
        }

        private static void CreateListsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Lists.TableName);
            storage.Environment.CreateTree(tx, storage.Lists.GetIndexKey(Tables.Lists.Indices.ByName));
            storage.Environment.CreateTree(tx, storage.Lists.GetIndexKey(Tables.Lists.Indices.ByNameAndKey));
        }

        private static void CreateQueuesSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Queues.TableName);
            storage.Environment.CreateTree(tx, storage.Queues.GetIndexKey(Tables.Queues.Indices.ByName));
            storage.Environment.CreateTree(tx, storage.Queues.GetIndexKey(Tables.Queues.Indices.Data));
        }

        internal static void CreateDocumentReferencesSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.DocumentReferences.TableName);
            storage.Environment.CreateTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByRef));
            storage.Environment.CreateTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByView));
            storage.Environment.CreateTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByViewAndKey));
            storage.Environment.CreateTree(tx, storage.DocumentReferences.GetIndexKey(Tables.DocumentReferences.Indices.ByKey));
        }

        internal static void CreateLastIndexedEtagsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.LastIndexedEtags.TableName);
        }

        internal static void CreateIndexingStatsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.IndexingStats.TableName);
        }

        private static void CreateDetailsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Details.TableName);
        }

        private static void CreateDocumentsSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.Documents.TableName);
            storage.Environment.CreateTree(tx, storage.Documents.GetIndexKey(Tables.Documents.Indices.KeyByEtag));
            storage.Environment.CreateTree(tx, storage.Documents.GetIndexKey(Tables.Documents.Indices.Metadata));
        }

        private static void CreateGeneralSchema(Transaction tx, TableStorage storage)
        {
            storage.Environment.CreateTree(tx, Tables.General.TableName);
        }
    }
}
