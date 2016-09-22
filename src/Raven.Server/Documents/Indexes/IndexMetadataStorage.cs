using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Replication.Messages;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
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

        private readonly Logger _logger;
        private readonly DocumentDatabase _documentDatabase;
        private readonly TableSchema _metadataSchema = new TableSchema();
        private ByteStringContext _allocator;
        private TransactionContextPool _contextPool;

        public StorageEnvironment StorageEnvironment { get; private set; }

        public IndexMetadataStorage(DocumentDatabase documentDatabase)
        {
            _documentDatabase = documentDatabase;
            _allocator = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes);
            
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

                    _metadataSchema.Create(tx,"IndexMetadata");
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

        public unsafe IndexMetadata ReadMetadata(int indexId)
        {            
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.ReadTransaction())
            {
                var table = tx.OpenTable(_metadataSchema, "IndexMetadata");
                var tvr = table.ReadByKey(Slice.External(_allocator, (byte*) &indexId, sizeof(int)));
                return TableValueToIndexMetadata(tvr);
            }
        }

        public unsafe void WriteMetadata(IndexMetadata metadata)
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.WriteTransaction())
            {
                var table = tx.OpenTable(_metadataSchema,"IndexMetadata");
                fixed (ChangeVectorEntry* pChangeVector = metadata.ChangeVector)
                {
                    table.Set(new TableValueBuilder
                    {
                        {(byte*) &metadata.IndexId, sizeof(int)},
                        {(byte*) pChangeVector, sizeof(ChangeVectorEntry) * metadata.ChangeVector.Length},
                        {(byte*) &metadata.Etag, sizeof(long)}
                    });
                }
                tx.Commit();
            }
        }

        public long ReadLastIndexEtag()
        {
            JsonOperationContext context;
            using (_contextPool.AllocateOperationContext(out context))
            using (var tx = StorageEnvironment.ReadTransaction())
            {
                return ReadLastEtag(tx);
            }
        }

        private unsafe long ReadLastEtag(Transaction tx)
        {
            var table = tx.OpenTable(_metadataSchema, "IndexMetadata");
            if (table.NumberOfEntries == 0)
                return 0;
            var result = table.SeekBackwardFrom(_metadataSchema.FixedSizeIndexes[EtagIndexName], long.MaxValue);
            var tvr = result.FirstOrDefault();
            if(tvr == null) //precaution
                throw new InvalidDataException("Tried to fetch last index etag and found no entries, despite NumberOfEntries > 0. This is not supposed to happen and is likely a bug.");

            int size;
            return *(long*) tvr.Read((int) IndexMetadataFields.Etag, out size);
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(IndexMetadataStorage)}");
            
            exceptionAggregator.Execute(() =>
            {
                _allocator.Dispose();
                _allocator = null;
            });

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
            metadata.Etag = *(long*) tvr.Read((int) IndexMetadataFields.Etag, out size);

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
