using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.ServerWide;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSmuggler : SmugglerBase
    {
        private readonly DocumentDatabase _database;

        public const string PreV4RevisionsDocumentId = "/revisions/";

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static bool IsPreV4Revision(BuildVersionType buildType, string id, Document document)
        {
            if (buildType == BuildVersionType.V3 == false)
                return false;

            if ((document.NonPersistentFlags & NonPersistentDocumentFlags.LegacyRevision) != NonPersistentDocumentFlags.LegacyRevision)
                return false;

            return id.Contains(PreV4RevisionsDocumentId, StringComparison.OrdinalIgnoreCase);
        }

        public DatabaseSmuggler(DocumentDatabase database, ISmugglerSource source, ISmugglerDestination destination, SystemTime time, JsonOperationContext context, DatabaseSmugglerOptionsServerSide options = null, SmugglerResult result = null, Action<IOperationProgress> onProgress = null, CancellationToken token = default) : 
            base(source, destination, time, context, options, result, onProgress, token)
        {
            _database = database;
            Debug.Assert((source is DatabaseSource && destination is DatabaseDestination) == false,
                "When both source and destination are database, we might get into a delayed write for the dest while the " +
                "source already pulsed its' read transaction, resulting in bad memory read.");
        }

        public override SmugglerPatcher CreatePatcher() => new DatabaseSmugglerPatcher(_options, _database);

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeAsync(SmugglerResult result)
        {
            result.CompareExchange.Start();
            await using (var actions = _destination.CompareExchange(_context))
            {
                await foreach (var kvp in _source.GetCompareExchangeValuesAsync())
                {
                    await InternalProcessCompareExchangeAsync(result, kvp, actions);
                }
            }

            return result.CompareExchange;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessCompareExchangeTombstonesAsync(SmugglerResult result)
        {
            result.CompareExchangeTombstones.Start();

            await using (var actions = _destination.CompareExchangeTombstones(_context))
            {
                await foreach (var key in _source.GetCompareExchangeTombstonesAsync())
                {
                    await InternalProcessCompareExchangeTombstonesAsync(result, key, actions);
                }
            }

            return result.CompareExchangeTombstones;
        }

        protected override async Task<SmugglerProgressBase.Counts> ProcessIndexesAsync(SmugglerResult result)
        {
            result.Indexes.Start();

            await using (var actions = _destination.Indexes())
            {
                await foreach (var index in _source.GetIndexesAsync())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Indexes.ReadCount++;

                    if (index == null)
                    {
                        result.Indexes.ErroredCount++;
                        continue;
                    }

                    if (OnIndexAction != null)
                    {
                        OnIndexAction(index);
                        continue;
                    }

                    switch (index.Type)
                    {
                        case IndexType.AutoMap:
                            var autoMapIndexDefinition = (AutoMapIndexDefinition)index.IndexDefinition;

                            try
                            {
                                await actions.WriteIndexAsync(autoMapIndexDefinition, IndexType.AutoMap);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write auto map index '{autoMapIndexDefinition.Name}': {e.Message}");
                            }
                            break;

                        case IndexType.AutoMapReduce:
                            var autoMapReduceIndexDefinition = (AutoMapReduceIndexDefinition)index.IndexDefinition;
                            try
                            {
                                await actions.WriteIndexAsync(autoMapReduceIndexDefinition, IndexType.AutoMapReduce);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write auto map-reduce index '{autoMapReduceIndexDefinition.Name}': {e.Message}");
                            }
                            break;

                        case IndexType.Map:
                        case IndexType.MapReduce:
                        case IndexType.JavaScriptMap:
                        case IndexType.JavaScriptMapReduce:
                            var indexDefinition = (IndexDefinition)index.IndexDefinition;
                            if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase))
                            {
                                result.AddInfo("Skipped 'Raven/DocumentsByEntityName' index. It is no longer needed.");
                                continue;
                            }

                            if (string.Equals(indexDefinition.Name, "Raven/ConflictDocuments", StringComparison.OrdinalIgnoreCase))
                            {
                                result.AddInfo("Skipped 'Raven/ConflictDocuments' index. It is no longer needed.");
                                continue;
                            }

                            if (indexDefinition.Name.StartsWith("Auto/", StringComparison.OrdinalIgnoreCase))
                            {
                                // legacy auto index
                                indexDefinition.Name = $"Legacy/{indexDefinition.Name}";
                            }

                            await WriteIndexAsync(result, indexDefinition, actions);
                            break;

                        case IndexType.Faulty:
                            break;

                        default:
                            throw new NotSupportedException(index.Type.ToString());
                    }

                    if (result.Indexes.ReadCount % 10 == 0)
                    {
                        var message = $"Read {result.Indexes.ReadCount:#,#;;0} indexes.";
                        AddInfoToSmugglerResult(result, message);
                    }
                }
            }

            return result.Indexes;
        }


        protected virtual async Task InternalProcessCompareExchangeAsync(SmugglerResult result, (CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value) kvp,
            ICompareExchangeActions actions)
        {
            _token.ThrowIfCancellationRequested();
            result.CompareExchange.ReadCount++;
            if (result.CompareExchange.ReadCount != 0 && result.CompareExchange.ReadCount % 1000 == 0)
                AddInfoToSmugglerResult(result, $"Read {result.CompareExchange.ReadCount:#,#;;0} compare exchange values.");

            if (kvp.Equals(default))
            {
                result.CompareExchange.ErroredCount++;
                return;
            }

            try
            {
                await actions.WriteKeyValueAsync(kvp.Key.Key, kvp.Value);
                result.CompareExchange.LastEtag = kvp.Index;
            }
            catch (Exception e)
            {
                result.CompareExchange.ErroredCount++;
                        result.AddError($"Could not write compare exchange with key: '{kvp.Key.Key}': {e.Message}");
            }
        }

        protected virtual async Task InternalProcessCompareExchangeTombstonesAsync(SmugglerResult result, (CompareExchangeKey Key, long Index) key, ICompareExchangeActions actions)
        {
            _token.ThrowIfCancellationRequested();
            result.CompareExchangeTombstones.ReadCount++;

            if (key.Equals(default))
            {
                result.CompareExchangeTombstones.ErroredCount++;
                return;
            }

            try
            {
                await actions.WriteTombstoneKeyAsync(key.Key.Key);
            }
            catch (Exception e)
            {
                result.CompareExchangeTombstones.ErroredCount++;
                result.AddError($"Could not write compare exchange '{key}: {e.Message}");
            }
        }
        
        protected async ValueTask WriteIndexAsync(SmugglerResult result, IndexDefinition indexDefinition, IIndexActions actions)
        {
            try
            {
                if (_options.RemoveAnalyzers)
                {
                    foreach (var indexDefinitionField in indexDefinition.Fields)
                        indexDefinitionField.Value.Analyzer = null;
                }

                await actions.WriteIndexAsync(indexDefinition);
            }
            catch (Exception e)
            {
                var exceptionMessage = e.Message;
                if (exceptionMessage.Contains("CS1501") && exceptionMessage.Contains("'LoadDocument'"))
                {
                    exceptionMessage =
                            "LoadDocument requires a second argument which is a collection name of the loaded document" + Environment.NewLine +
                            "For example: " + Environment.NewLine +
                                "\tfrom doc in doc.Orders" + Environment.NewLine +
                                "\tlet company = LoadDocument(doc.Company, \"Companies\")" + Environment.NewLine +
                                "\tselect new {" + Environment.NewLine +
                                    "\t\tCompanyName: company.Name" + Environment.NewLine +
                                "\t}" + Environment.NewLine +
                            exceptionMessage + Environment.NewLine;
                }
                else if (exceptionMessage.Contains("CS0103") &&
                         (exceptionMessage.Contains("'AbstractIndexCreationTask'") ||
                          exceptionMessage.Contains("'SpatialIndex'")))
                {
                    exceptionMessage = "'AbstractIndexCreationTask.SpatialGenerate' can be replaced with 'CreateSpatialField'" + Environment.NewLine +
                                       "'SpatialIndex.Generate' can be replaced with 'CreateSpatialField'" + Environment.NewLine +
                                       exceptionMessage + Environment.NewLine;
                }
                else if (exceptionMessage.Contains("CS0234") && exceptionMessage.Contains("'Abstractions'"))
                {
                    exceptionMessage = "'Raven.Abstractions.Linq.DynamicList' can be removed" + Environment.NewLine +
                                       $"{exceptionMessage}" + Environment.NewLine;
                }

                result.Indexes.ErroredCount++;
                var errorMessage = $"Could not write index '{indexDefinition.Name}', error: {exceptionMessage}" + Environment.NewLine +
                                   $"Maps: [{Environment.NewLine}{string.Join($", {Environment.NewLine}", indexDefinition.Maps)}{Environment.NewLine}]";

                if (indexDefinition.Reduce != null)
                {
                    errorMessage += Environment.NewLine + $"Reduce: {indexDefinition.Reduce}";
                }

                result.AddError(errorMessage);
            }
        }
    }
}
