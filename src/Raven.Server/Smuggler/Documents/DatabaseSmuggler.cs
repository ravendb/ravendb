using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Smuggler.Documents.Data;
using Raven.Server.Smuggler.Documents.Processors;

namespace Raven.Server.Smuggler.Documents
{
    public class DatabaseSmuggler
    {
        private readonly ISmugglerSource _source;
        private readonly ISmugglerDestination _destination;
        private readonly DatabaseSmugglerOptions _options;
        private readonly SmugglerResult _result;
        private readonly SystemTime _time;
        private readonly Action<IOperationProgress> _onProgress;
        private readonly SmugglerPatcher _patcher;
        private CancellationToken _token;

        public DatabaseSmuggler(
            ISmugglerSource source,
            ISmugglerDestination destination,
            SystemTime time,
            DatabaseSmugglerOptions options = null,
            SmugglerResult result = null,
            Action<IOperationProgress> onProgress = null,
            CancellationToken token = default(CancellationToken))
        {
            _source = source;
            _destination = destination;
            _options = options ?? new DatabaseSmugglerOptions();
            _result = result;
            _token = token;

            if (string.IsNullOrWhiteSpace(_options.TransformScript) == false)
                _patcher = new SmugglerPatcher(_options);

            _time = time;
            _onProgress = onProgress ?? (progress => { });
        }

        public SmugglerResult Execute()
        {
            var result = _result ?? new SmugglerResult();

            using (_source.Initialize(_options, result, out long buildVersion))
            using (_destination.Initialize(_options, result, buildVersion))
            {
                var buildType = BuildVersion.Type(buildVersion);
                var currentType = _source.GetNextType();
                while (currentType != DatabaseItemType.None)
                {
                    ProcessType(currentType, result, buildType);

                    currentType = _source.GetNextType();
                }

                EnsureStepProcessed(result.Documents);
                EnsureStepProcessed(result.Documents.Attachments);
                EnsureStepProcessed(result.RevisionDocuments);
                EnsureStepProcessed(result.RevisionDocuments.Attachments);
                EnsureStepProcessed(result.Indexes);
                EnsureStepProcessed(result.Transformers);
                EnsureStepProcessed(result.LocalIdentities);
                EnsureStepProcessed(result.ClusterIdentities);

                return result;
            }
        }

        private static void EnsureStepProcessed(SmugglerProgressBase.Counts counts)
        {
            if (counts.Processed)
                return;

            counts.Processed = true;
            counts.Skipped = true;
        }

        private void ProcessType(DatabaseItemType type, SmugglerResult result, BuildVersionType buildType)
        {
            if ((_options.OperateOnTypes & type) != type)
            {
                SkipType(type, result);
                return;
            }

            result.AddInfo($"Started processing {type}.");
            _onProgress.Invoke(result.Progress);

            SmugglerProgressBase.Counts counts;
            switch (type)
            {
                case DatabaseItemType.Documents:
                    counts = ProcessDocuments(result, buildType);
                    break;
                case DatabaseItemType.RevisionDocuments:
                    counts = ProcessRevisionDocuments(result);
                    break;
                case DatabaseItemType.Indexes:
                    counts = ProcessIndexes(result);
                    break;
                case DatabaseItemType.Transformers:
                    counts = ProcessTransformers(result);
                    break;
                case DatabaseItemType.LocalIdentities:
                    counts = ProcessIdentities(result, IdentityType.Local);
                    break;
                case DatabaseItemType.ClusterIdentities:
                    counts = ProcessIdentities(result, IdentityType.Cluster);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            counts.Processed = true;

            var countsWithEtag = counts as SmugglerProgressBase.CountsWithLastEtag;
            if (countsWithEtag != null)
                countsWithEtag.Attachments.Processed = true;

            result.AddInfo($"Finished processing {type}. {counts}");
            _onProgress.Invoke(result.Progress);
        }

        private void SkipType(DatabaseItemType type, SmugglerResult result)
        {
            result.AddInfo($"Skipping '{type}' processing.");
            _onProgress.Invoke(result.Progress);

            var numberOfItemsSkipped = _source.SkipType(type);

            SmugglerProgressBase.Counts counts;
            switch (type)
            {
                case DatabaseItemType.Documents:
                    counts = result.Documents;
                    break;
                case DatabaseItemType.RevisionDocuments:
                    counts = result.RevisionDocuments;
                    break;
                case DatabaseItemType.Indexes:
                    counts = result.Indexes;
                    break;
                case DatabaseItemType.Transformers:
                    counts = result.Transformers;
                    break;
                case DatabaseItemType.LocalIdentities:
                    counts = result.LocalIdentities;
                    break;
                case DatabaseItemType.ClusterIdentities:
                    counts = result.ClusterIdentities;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            counts.Skipped = true;
            counts.Processed = true;

            if (numberOfItemsSkipped > 0)
            {
                counts.ReadCount = numberOfItemsSkipped;
                result.AddInfo($"Skipped '{type}' processing. Skipped {numberOfItemsSkipped} items.");
            }
            else
                result.AddInfo($"Skipped '{type}' processing.");

            _onProgress.Invoke(result.Progress);
        }

        private SmugglerProgressBase.Counts ProcessIdentities(SmugglerResult result, IdentityType type)
        {
            switch (type)
            {
                case IdentityType.Local:
                    using (var localIdentityActions = _destination.LocalIdentities())
                    {
                        var sourceIdentities = _source.GetLocalIdentities();
                        WriteIdentities(result, sourceIdentities, localIdentityActions,
                            () => result.LocalIdentities.ReadCount++,
                            () => result.LocalIdentities.ErroredCount++);
                    }
                    break;
                case IdentityType.Cluster:
                    using (var clusterIdentityActions = _destination.ClusterIdentities())
                    {
                        var sourceIdentities = _source.GetClusterIdentities();
                        WriteIdentities(result, sourceIdentities, clusterIdentityActions,
                                () => result.ClusterIdentities.ReadCount++,
                                () => result.ClusterIdentities.ErroredCount++
                            );
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(type), type, null);
            }

            return type == IdentityType.Local ? result.LocalIdentities : result.ClusterIdentities;
        }

        private void WriteIdentities(SmugglerResult result, 
            IEnumerable<KeyValuePair<string, long>> sourceIdentities, 
            IIdentityActions destinationIdentityActions,
            Action incrementReadCount,
            Action incrementErrorCount)
        {
            foreach (var kvp in sourceIdentities)
            {
                _token.ThrowIfCancellationRequested();
                incrementReadCount();

                if (kvp.Equals(default(KeyValuePair<string, long>)))
                {
                    incrementErrorCount();
                    continue;
                }

                try
                {
                    destinationIdentityActions.WriteIdentity(kvp.Key, kvp.Value);
                }
                catch (Exception e)
                {
                    incrementErrorCount();
                    result.AddError($"Could not write identity (of type {destinationIdentityActions.Type}) '{kvp.Key} {kvp.Value}': {e.Message}");
                }
            }
        }

        private SmugglerProgressBase.Counts ProcessTransformers(SmugglerResult result)
        {
            using (var actions = _destination.Transformers())
            {
                foreach (var transformer in _source.GetTransformers())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Transformers.ReadCount++;

                    if (transformer == null)
                    {
                        result.Transformers.ErroredCount++;
                        continue;
                    }

                    try
                    {
                        actions.WriteTransformer(transformer);
                    }
                    catch (Exception e)
                    {
                        result.Transformers.ErroredCount++;
                        result.AddError($"Could not write transformer '{transformer.Name}': {e.Message}");
                    }
                }
            }

            return result.Transformers;
        }

        private SmugglerProgressBase.Counts ProcessIndexes(SmugglerResult result)
        {
            using (var actions = _destination.Indexes())
            {
                foreach (var index in _source.GetIndexes())
                {
                    _token.ThrowIfCancellationRequested();
                    result.Indexes.ReadCount++;

                    if (index == null)
                    {
                        result.Indexes.ErroredCount++;
                        continue;
                    }

                    switch (index.Type)
                    {
                        case IndexType.AutoMap:
                            var autoMapIndexDefinition = (AutoMapIndexDefinition)index.IndexDefinition;

                            try
                            {
                                actions.WriteIndex(autoMapIndexDefinition, IndexType.AutoMap);
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
                                actions.WriteIndex(autoMapReduceIndexDefinition, IndexType.AutoMapReduce);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write auto map-reduce index '{autoMapReduceIndexDefinition.Name}': {e.Message}");
                            }
                            break;
                        case IndexType.Map:
                        case IndexType.MapReduce:
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

                            try
                            {
                                if (_options.RemoveAnalyzers)
                                {
                                    foreach (var indexDefinitionField in indexDefinition.Fields)
                                        indexDefinitionField.Value.Analyzer = null;
                                }

                                actions.WriteIndex(indexDefinition);
                            }
                            catch (Exception e)
                            {
                                result.Indexes.ErroredCount++;
                                result.AddError($"Could not write index '{indexDefinition.Name}': {e.Message}");
                            }
                            break;
                        case IndexType.Faulty:
                            break;
                        default:
                            throw new NotSupportedException(index.Type.ToString());
                    }
                }
            }

            return result.Indexes;
        }

        private SmugglerProgressBase.Counts ProcessRevisionDocuments(SmugglerResult result)
        {
            using (var actions = _destination.RevisionDocuments())
            {
                foreach (var item in _source.GetRevisionDocuments(_options.CollectionsToExport, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.RevisionDocuments.ReadCount++;

                    if (result.RevisionDocuments.ReadCount % 1000 == 0)
                    {
                        result.AddInfo($"Read {result.RevisionDocuments.ReadCount:#,#;;0} documents.");
                        _onProgress.Invoke(result.Progress);
                    }

                    if (item.Document == null)
                    {
                        result.RevisionDocuments.ErroredCount++;
                        continue;
                    }

                    Debug.Assert(item.Document.Id != null);

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    actions.WriteDocument(item, result.RevisionDocuments);

                    result.RevisionDocuments.LastEtag = item.Document.Etag;
                }
            }

            return result.RevisionDocuments;
        }

        private SmugglerProgressBase.Counts ProcessDocuments(SmugglerResult result, BuildVersionType buildType)
        {
            using (var actions = _destination.Documents())
            {
                foreach (var item in _source.GetDocuments(_options.CollectionsToExport, actions))
                {
                    _token.ThrowIfCancellationRequested();
                    result.Documents.ReadCount++;

                    if (result.Documents.ReadCount % 1000 == 0)
                    {
                        var message = $"Read {result.Documents.ReadCount:#,#;;0} documents.";
                        if (result.Documents.Attachments.ReadCount > 0)
                            message += $" Read {result.Documents.Attachments.ReadCount:#,#;;0} attachments.";
                        result.AddInfo(message);
                        _onProgress.Invoke(result.Progress);
                    }

                    if (item.Document == null)
                    {
                        result.Documents.ErroredCount++;
                        continue;
                    }

                    if (item.Document.Id == null)
                        ThrowInvalidData();

                    if (CanSkipDocument(item.Document, buildType))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_options.IncludeExpired == false && item.Document.Expired(_time.GetUtcNow()))
                    {
                        SkipDocument(item, result);
                        continue;
                    }

                    if (_patcher != null)
                    {
                        item.Document = _patcher.Transform(item.Document, actions.GetContextForNewDocument());
                        if (item.Document == null)
                        {
                            result.Documents.SkippedCount++;
                            continue;
                        }
                    }

                    // TODO: RavenDB-6931 - Make sure that patching cannot change the @attachments and @collectoin in metadata

                    item.Document.NonPersistentFlags |= NonPersistentDocumentFlags.FromSmuggler;

                    actions.WriteDocument(item, result.Documents);

                    result.Documents.LastEtag = item.Document.Etag;
                }
            }

            return result.Documents;
        }

        private void SkipDocument(DocumentItem item, SmugglerResult result)
        {
            result.Documents.SkippedCount++;

            if (item.Document != null)
            {
                item.Document.Data.Dispose();

                if (item.Attachments != null)
                {
                    foreach (var attachment in item.Attachments)
                    {
                        attachment.Dispose();
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool CanSkipDocument(Document document, BuildVersionType buildType)
        {
            if (buildType != BuildVersionType.V3)
                return false;

            // skipping "Raven/Replication/DatabaseIdsCache" and
            // "Raven/Replication/Sources/{GUID}"
            if (document.Id.Size != 34 && document.Id.Size != 62)
                return false;

            return document.Id == "Raven/Replication/DatabaseIdsCache" ||
                   document.Id.StartsWith("Raven/Replication/Sources/");
        }

        private static void ThrowInvalidData()
        {
            throw new InvalidDataException("Document does not contain an id.");
        }
    }
}