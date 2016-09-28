using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes
{
    public class IndexMetadataStorage : IDisposable
    {
        private static readonly Slice IndexIdKeyName = Slice.From(StorageEnvironment.LabelsContext, "IndexIdKey", ByteStringType.Immutable);
        private static readonly Slice ChangeVectorIndexName = Slice.From(StorageEnvironment.LabelsContext, "ChangeVectorIndex", ByteStringType.Immutable);
        private static readonly Slice EtagIndexName = Slice.From(StorageEnvironment.LabelsContext, "EtagIndex", ByteStringType.Immutable);
        private const string GlobalChangeVectorTreeName = "GlobalChangeVector";

        private readonly Logger _logger;
        private readonly DocumentDatabase _documentDatabase;
        private readonly TableSchema _metadataSchema = new TableSchema();
        private TransactionContextPool _contextPool;

        public StorageEnvironment StorageEnvironment { get; private set; }

        public IndexMetadataStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            
            _logger = LoggingSource.Instance.GetLogger<IndexMetadataStorage>("IndexMetadataStore_Of_" + _documentDatabase.Name);
        }


        public void Initialize()
        {
            if (_logger.IsInfoEnabled)
                _logger.Info
                    ("Starting to open index metadata storage for " + (_documentDatabase.Configuration.Indexing.RunInMemory ?
                    "<memory>" : _documentDatabase.Configuration.Indexing.IndexStoragePath));
            var options = _documentDatabase.Configuration.Indexing.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly()
                : StorageEnvironmentOptions.ForPath(_documentDatabase.Configuration.Indexing.IndexStoragePath);

            try
            {
                Initialize(options);
            }
            catch (Exception)
            {
                options.Dispose();
                throw;
            }
        }

        public void Initialize(StorageEnvironmentOptions options)
        {
            options.SchemaVersion = 1;
            try
            {
                StorageEnvironment = new StorageEnvironment(options);
                _contextPool = new TransactionContextPool(StorageEnvironment);

                using (var tx = StorageEnvironment.WriteTransaction())
                {
                    _metadataSchema.DefineKey(new TableSchema.SchemaIndexDef
                    {
                        IsGlobal = true,
                        StartIndex = 0,
                        Count = 1,
                        Name = IndexIdKeyName
                    });

                    _metadataSchema.DefineIndex(new TableSchema.SchemaIndexDef
                    {
                        IsGlobal = true,
                        Count = 1,
                        StartIndex = 1,
                        Name = ChangeVectorIndexName,
                    });

                    _metadataSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
                    {
                        IsGlobal = true,
                        StartIndex = 2,
                        Name = EtagIndexName,
                    });
                    _metadataSchema.Create(tx, "IndexMetadata");
                    tx.CreateTree(GlobalChangeVectorTreeName);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open index metadata store for " + _documentDatabase.Name, e);

                options.Dispose();
                Dispose();
                throw;
            }
        }

        public unsafe IndexMetadata WriteNewMetadataFor(int indexId)
        {
            JsonOperationContext context;
            var metadata = new IndexMetadata();
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.WriteTransaction())
            {
                var indexMetadataTable = tx.OpenTable(_metadataSchema, "IndexMetadata");
                var result = indexMetadataTable.ReadByKey(Slice.External(context.Allocator, (byte*) &indexId, sizeof(int)));
                if (result != null)
                    return TableValueToIndexMetadata(result);

                var lastEtag = ReadLastEtag(indexMetadataTable);
                var globalChangeVectorTree = tx.ReadTree(GlobalChangeVectorTreeName);
                var globalChangeVector = ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree);

                metadata.Etag = lastEtag + 1;
                metadata.ChangeVector = ReplicationUtils.UpdateChangeVectorWithNewEtag(
                    StorageEnvironment.DbId, metadata.Etag, globalChangeVector);
                metadata.IndexId = indexId;

                WriteMetadataInternal(metadata,tx,context);

                var newGlobalChangeVector = ReplicationUtils.MergeVectors(globalChangeVector, metadata.ChangeVector);
                ReplicationUtils.WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);

                tx.Commit();
            }

            return metadata;
        }

        public IndexMetadata ReadMetadata(int indexId)
        {            
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.ReadTransaction())
                return ReadMetadataInternal(indexId, tx, context);
        }

        private unsafe IndexMetadata ReadMetadataInternal(int indexId, Transaction tx, JsonOperationContext context)
        {
            var table = tx.OpenTable(_metadataSchema, "IndexMetadata");
            var tvr = table.ReadByKey(Slice.External(context.Allocator, (byte*) &indexId, sizeof(int)));
            return TableValueToIndexMetadata(tvr);
        }

        public void WriteMetadata(IndexMetadata metadata)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.WriteTransaction())
            {
                WriteMetadataInternal(metadata, tx, context);
                IndexMetadata metadata1 = metadata;
                var globalChangeVectorTree = tx.ReadTree(GlobalChangeVectorTreeName);
                var newGlobalChangeVector = ReplicationUtils.MergeVectors(
                    ReplicationUtils.ReadChangeVectorFrom(globalChangeVectorTree),
                    metadata1.ChangeVector);
                ReplicationUtils.WriteChangeVectorTo(context, newGlobalChangeVector, globalChangeVectorTree);

                tx.Commit();
            }
        }

        private unsafe void WriteMetadataInternal(IndexMetadata metadata, Transaction tx, JsonOperationContext context)
        {
            var table = tx.OpenTable(_metadataSchema, "IndexMetadata");
            fixed (ChangeVectorEntry* pChangeVector = metadata.ChangeVector)
            {
                var etag = Bits.SwapBytes(metadata.Etag);
                table.Set(new TableValueBuilder
                {
                    {(byte*) &metadata.IndexId, sizeof(int)},
                    {(byte*) pChangeVector, sizeof(ChangeVectorEntry)*metadata.ChangeVector.Length},
                    {(byte*) &etag, sizeof(long)}
                });              
            }
        }

        public unsafe void DeleteMetadata(int indexId)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.WriteTransaction())
            {
                var table = tx.OpenTable(_metadataSchema, "IndexMetadata");
                table.DeleteByKey(Slice.External(context.Allocator,(byte*)&indexId,sizeof(int)));
                tx.Commit();
            }
        }

        public IEnumerable<IndexMetadata> GetIndexMetadataAfter(long etag, int start = 0, int take = 1024)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.ReadTransaction())
            {
                var indexMetadataTable = tx.OpenTable(_metadataSchema, "IndexMetadata");
                foreach (var tvr in indexMetadataTable.SeekForwardFrom(
                                            _metadataSchema.
                                                FixedSizeIndexes[EtagIndexName], etag))
                {

                    if (start > 0)
                    {
                        start--;
                        continue;
                    }
                    if (take-- <= 0)
                    {
                        yield break;
                    }

                    yield return TableValueToIndexMetadata(tvr);
                }
            }
        }

        public long ReadLastIndexEtag()
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.ReadTransaction())
                return ReadLastEtag(tx.OpenTable(_metadataSchema, "IndexMetadata"));
        }

        private unsafe long ReadLastEtag(Table metadataSchemaTable)
        {
            if (metadataSchemaTable.NumberOfEntries == 0)
                return 0;
            var result = metadataSchemaTable.SeekBackwardFrom(_metadataSchema.FixedSizeIndexes[EtagIndexName], long.MaxValue);
            var tvr = result.FirstOrDefault();
            if(tvr == null) //precaution
                throw new InvalidDataException("Tried to fetch last index etag and found no entries, despite NumberOfEntries > 0. This is not supposed to happen and is likely a bug.");

            int size;
            return IPAddress.NetworkToHostOrder(*(long*) tvr.Read((int) IndexMetadataFields.Etag, out size));
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(IndexMetadataStorage)}");
            
            exceptionAggregator.Execute(() =>
            {
                StorageEnvironment?.Dispose();
                StorageEnvironment = null;
            });
            exceptionAggregator.ThrowIfNeeded();
        }

        private unsafe IndexMetadata TableValueToIndexMetadata(TableValueReader tvr)
        {
            var metadata = new IndexMetadata();

            int size;
            metadata.IndexId = *(int*) tvr.Read((int) IndexMetadataFields.IndexId, out size);
            metadata.ChangeVector = StorageUtil.GetChangeVectorEntriesFromTableValueReader(tvr,(int) IndexMetadataFields.ChangeVector);
            metadata.Etag = IPAddress.NetworkToHostOrder(*(long*)tvr.Read((int)IndexMetadataFields.Etag, out size)); 

            return metadata;
        }

        private enum IndexMetadataFields
        {
            IndexId = 0,
            ChangeVector = 1,
            Etag = 2
        }
    }
}
