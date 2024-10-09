﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Processors.Debugging;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;
using Voron;
using Voron.Data;
using Voron.Data.Fixed;
using Voron.Debugging;
using Voron.Impl;

namespace Raven.Server.Documents.Handlers.Debugging
{
    public sealed class StorageHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/storage/manual-flush", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task ManualFlush()
        {
            var name = GetStringQueryString("name");
            var typeAsString = GetStringQueryString("type");

            if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
                throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);

            var env = Database.GetAllStoragesEnvironment()
                .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);

            if (env == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            GlobalFlushingBehavior.GlobalFlusher.Value.MaybeFlushEnvironment(env.Environment);

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/storage/manual-sync", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task ManualSync()
        {
            var name = GetStringQueryString("name");
            var typeAsString = GetStringQueryString("type");

            if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
                throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);

            var env = Database.GetAllStoragesEnvironment()
                .FirstOrDefault(x => string.Equals(x.Name, name, StringComparison.OrdinalIgnoreCase) && x.Type == type);

            if (env == null)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return Task.CompletedTask;
            }

            env.Environment.ForceSyncDataFile();

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/debug/storage/environment/debug-only/pages", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task Pages()
        {
            using (var processor = new StorageHandlerProcessorForGetEnvironmentPages(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/storage/trees", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task Trees()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("Results");
                    writer.WriteStartArray();
                    var first = true;

                    foreach (var treeType in new[] { RootObjectType.VariableSizeTree, RootObjectType.FixedSizeTree, RootObjectType.EmbeddedFixedSizeTree })
                    {
                        foreach (var name in GetTreeNames(tx.InnerTransaction, treeType))
                        {
                            if (first == false)
                                writer.WriteComma();

                            first = false;

                            writer.WriteStartObject();

                            writer.WritePropertyName("Name");
                            writer.WriteString(name);
                            writer.WriteComma();

                            writer.WritePropertyName("Type");
                            writer.WriteString(treeType.ToString());

                            writer.WriteEndObject();
                        }
                    }

                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }

        [RavenAction("/databases/*/debug/storage/btree-structure", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task BTreeStructure()
        {
            var treeName = GetStringQueryString("name", required: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree(treeName)
                    ?? throw new InvalidOperationException("Tree name '" + treeName + "' was not found. Existing trees: " +
                        string.Join(", ", GetTreeNames(tx.InnerTransaction, RootObjectType.VariableSizeTree))
                    );

                HttpContext.Response.ContentType = "text/html";
                await DebugStuff.DumpTreeToStreamAsync(tree, ResponseBodyStream());
            }
        }

        [RavenAction("/databases/*/debug/storage/fst-structure", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task FixedSizeTreeStructure()
        {
            var treeName = GetStringQueryString("name", required: true);

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.OpenReadTransaction())
            {
                FixedSizeTree tree;
                try
                {
                    tree = tx.InnerTransaction.FixedTreeFor(treeName);
                }
                catch (Exception e)
                {
                    throw new InvalidOperationException("Existing trees: " +
                            string.Join(", ", GetTreeNames(tx.InnerTransaction, RootObjectType.FixedSizeTree))
                        , e);
                }

                HttpContext.Response.ContentType = "text/html";
                await DebugStuff.DumpFixedSizedTreeToStreamAsync(tx.InnerTransaction.LowLevelTransaction, tree, ResponseBodyStream());
            }
        }

        private static IEnumerable<string> GetTreeNames(Transaction tx, RootObjectType type)
        {
            using (var rootIterator = tx.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    if (tx.GetRootObjectType(rootIterator.CurrentKey) != type)
                        continue;

                    yield return rootIterator.CurrentKey.ToString();
                } while (rootIterator.MoveNext());
            }
        }

        [RavenAction("/databases/*/debug/storage/report", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task Report()
        {
            using (var processor = new StorageHandlerProcessorForGetReport(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/storage/all-environments/report", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task AllEnvironmentsReport()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName("DatabaseName");
                    writer.WriteString(Database.Name);
                    writer.WriteComma();

                    writer.WritePropertyName("Environments");
                    writer.WriteStartArray();
                    WriteAllEnvs(writer, context);
                    writer.WriteEndArray();

                    writer.WriteEndObject();
                }
            }
        }

        private void WriteAllEnvs<TWriter>(TWriter writer, DocumentsOperationContext context)
            where TWriter : IBlittableJsonTextWriter
        {
            var envs = Database.GetAllStoragesEnvironment();

            bool first = true;
            foreach (var env in envs)
            {
                if (env == null)
                    continue;

                if (!first)
                    writer.WriteComma();
                first = false;

                writer.WriteStartObject();
                writer.WritePropertyName("Environment");
                writer.WriteString(env.Name);
                writer.WriteComma();

                writer.WritePropertyName("Type");
                writer.WriteString(env.Type.ToString());
                writer.WriteComma();

                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(GetDetailedReport(env, false));
                writer.WritePropertyName("Report");
                writer.WriteObject(context.ReadObject(djv, env.Name));

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/debug/storage/environment/report", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetEnvironmentReport()
        {
            using (var processor = new StorageHandlerProcessorForGetEnvironmentReport(this))
                await processor.ExecuteAsync();
        }

        private DetailedStorageReport GetDetailedReport(StorageEnvironmentWithType environment, bool details)
        {
            if (environment.Type != StorageEnvironmentWithType.StorageEnvironmentType.Index)
            {
                using (var tx = environment.Environment.ReadTransaction())
                {
                    return environment.Environment.GenerateDetailedReport(tx, details);
                }
            }

            var index = Database.IndexStore.GetIndex(environment.Name);
            return index.GenerateStorageReport(details);
        }

        [RavenAction("/databases/*/debug/storage/compression-dictionaries", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = false)]
        public async Task CompressionDictionary()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (var tx = context.Environment.ReadTransaction())
            await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName("DatabaseName");
                writer.WriteString(Database.Name);
                writer.WriteComma();

                var inMemoryCompressionDictionaries = context.Environment.CompressionDictionariesHolder.CompressionDictionaries;
                WriteCompressionDictionaries(writer, inMemoryCompressionDictionaries.Values.ToArray(), "InMemory");

                writer.WriteComma();

                var inStorageDictionaries = context.Environment.CompressionDictionariesHolder.GetInStorageDictionaries(tx);
                WriteCompressionDictionaries(writer, inStorageDictionaries.ToArray(), "InStorage");

                writer.WriteEndObject();
            }
        }

        private static void WriteCompressionDictionaries(AsyncBlittableJsonTextWriterForDebug writer, ZstdLib.CompressionDictionary[] dictionaries, string sourceName)
        {
            writer.WritePropertyName(sourceName);

            writer.WriteStartObject();
            writer.WritePropertyName("NumberOfEntries");
            writer.WriteInteger(dictionaries.Length);
            writer.WriteComma();

            writer.WritePropertyName("Entries");
            writer.WriteStartArray();

            bool firstInMemoryEntry = true;
            foreach (var dict in dictionaries)
            {
                if (firstInMemoryEntry == false)
                    writer.WriteComma();
                firstInMemoryEntry = false;

                writer.WriteStartObject();
                writer.WritePropertyName("Id");
                writer.WriteInteger(dict.Id);
#if DEBUG
                writer.WriteComma();
                writer.WritePropertyName("DictionaryHash");
                writer.WriteString(dict.DictionaryHash);
#endif
                writer.WriteEndObject();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        [RavenAction("/databases/*/debug/storage/environment/scratch-buffer-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task ScratchBufferPoolInfoReport()
        {
            using (var processor = new StorageHandlerProcessorForGetScratchBufferReport(this))
                await processor.ExecuteAsync();
        }
        
        [RavenAction("/databases/*/debug/storage/environment/free-space-snapshot", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetFreeSpaceInfo()
        {
            var name = GetStringQueryString("name", false) ?? Database.Name;
            var typeAsString = GetStringQueryString("type", false) ?? nameof(StorageEnvironmentWithType.StorageEnvironmentType.Documents);
            if (Enum.TryParse(typeAsString, out StorageEnvironmentWithType.StorageEnvironmentType type) == false)
                throw new InvalidOperationException("Query string value 'type' is not a valid environment type: " + typeAsString);

            var storage = Database.GetAllStoragesEnvironment().FirstOrDefault(x => x.Name == name && x.Type == type);
            if (storage == null)
                throw new InvalidOperationException($"The storage with name '{name}' and type '{type}' was not found.");
                    
            var hex = this.GetBoolValueQueryString("hex", false) ?? true;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var freeSpaceHandling = storage.Environment.FreeSpaceHandling;
                await using (var write = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, ResponseBodyStream()))
                {
                    context.Write(write, new DynamicJsonValue()
                    {
                        ["FreePages"] = freeSpaceHandling.FreeSpaceSnapshot(context.Transaction.InnerTransaction.LowLevelTransaction, hex)
                    });
                }
            }
        }
    }
}
