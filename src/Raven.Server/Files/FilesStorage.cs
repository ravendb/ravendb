using System;
using System.IO;
using System.Runtime.CompilerServices;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Voron;
using Voron.Data.Tables;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Logging;
using Voron.Data.Fixed;
using Voron.Exceptions;
using Voron.Impl;

namespace Raven.Server.Files
{
    public unsafe class FilesStorage : IDisposable
    {
        private static readonly Slice FilesSlice;
        private static readonly Slice FilesMetadataSlice;
        private static readonly Slice EtagsSlice;
        private static readonly Slice FilesEtagSlice;
        private static readonly Slice LastEtagSlice;
        private static readonly TableSchema FilesSchema = new TableSchema();

        private readonly FileSystem _fileSystem;
        private readonly string _name;
        private readonly Logger _logger;

        public FilesContextPool ContextPool;

        // this is only modified by write transactions under lock
        // no need to use thread safe ops
        private long _lastEtag;
        private const int StreamIdentifierPosition = 6;

        public FilesStorage(FileSystem fileSystem)
        {
            _fileSystem = fileSystem;
            _name = _fileSystem.Name;
            _logger = LoggingSource.Instance.GetLogger<FilesStorage>(fileSystem.Name);
        }

        static FilesStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Files", ByteStringType.Immutable, out FilesSlice);
            Slice.From(StorageEnvironment.LabelsContext, "FilesMetadata", ByteStringType.Immutable, out FilesMetadataSlice);
            Slice.From(StorageEnvironment.LabelsContext, "Etags", ByteStringType.Immutable, out EtagsSlice);
            Slice.From(StorageEnvironment.LabelsContext, "FilesEtagSlice", ByteStringType.Immutable, out FilesEtagSlice);
            Slice.From(StorageEnvironment.LabelsContext, "LastEtag", ByteStringType.Immutable, out LastEtagSlice);

            FilesSchema.DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                IsGlobal = false,
                Name = FilesSlice
            });
            FilesSchema.DefineFixedSizeIndex(new TableSchema.FixedSizeSchemaIndexDef
            {
                StartIndex = 1,
                IsGlobal = false,
                Name = FilesEtagSlice
            });
        }

        public StorageEnvironment Environment { get; private set; }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator(_logger, $"Could not dispose {nameof(DocumentsStorage)}");

            exceptionAggregator.Execute(() =>
            {
                ContextPool?.Dispose();
                ContextPool = null;
            });

            exceptionAggregator.Execute(() =>
            {
                Environment?.Dispose();
                Environment = null;
            });

            exceptionAggregator.ThrowIfNeeded();
        }

        public void Initialize()
        {
            if (_logger.IsInfoEnabled)
            {
                _logger.Info
                    ("Starting to open document storage for " + (_fileSystem.Configuration.Core.RunInMemory ?
                    "<memory>" : _fileSystem.Configuration.Core.DataDirectory.FullPath));
            }

            var options = _fileSystem.Configuration.Core.RunInMemory
                ? StorageEnvironmentOptions.CreateMemoryOnly(_fileSystem.Configuration.Core.DataDirectory.FullPath)
                : StorageEnvironmentOptions.ForPath(
                    _fileSystem.Configuration.Core.DataDirectory.FullPath,
                    _fileSystem.Configuration.Storage.TempPath?.FullPath,
                    _fileSystem.Configuration.Storage.JournalsStoragePath?.FullPath
                    );

            options.ForceUsing32BitPager = _fileSystem.Configuration.Storage.ForceUsing32BitPager;

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
                Environment = new StorageEnvironment(options);
                ContextPool = new FilesContextPool(_fileSystem);

                using (var tx = Environment.WriteTransaction())
                {
                    NewPageAllocator.MaybePrefetchSections(
                        tx.LowLevelTransaction.RootObjects,
                        tx.LowLevelTransaction);

                    tx.CreateTree(FilesSlice);

                    FilesSchema.Create(tx, FilesMetadataSlice, 32);

                    _lastEtag = ReadLastEtag(tx);

                    tx.Commit();
                }
            }
            catch (Exception e)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("Could not open server store for " + _name, e);

                Dispose();
                options.Dispose();
                throw;
            }
        }

        private long ReadLastEtag(Transaction tx)
        {
            var tree = tx.CreateTree(EtagsSlice);
            var readResult = tree.Read(LastEtagSlice);
            long lastEtag = 0;
            if (readResult != null)
                lastEtag = readResult.Reader.ReadLittleEndianInt64();

            var lastDocumentEtag = ReadLastFileEtag(tx);
            if (lastDocumentEtag > lastEtag)
                lastEtag = lastDocumentEtag;

            return lastEtag;
        }

        public static long ReadLastFileEtag(Transaction tx)
        {
            return ReadLastEtagFrom(tx, FilesEtagSlice);
        }

        private static long ReadLastEtagFrom(Transaction tx, Slice name)
        {
            using (var fst = new FixedSizeTree(tx.LowLevelTransaction,
                tx.LowLevelTransaction.RootObjects,
                name, sizeof(long),
                clone: false))
            {
                using (var it = fst.Iterate())
                {
                    if (it.SeekToLast())
                        return it.CurrentKey;
                }
            }

            return 0;
        }

        public FileSystemFile Get(FilesOperationContext context, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Argument is null or whitespace", nameof(name));
            if (context.Transaction == null)
                throw new ArgumentException("Context must be set with a valid transaction before calling Get", nameof(context));

            Slice loweredKey;
            using (DocumentKeyWorker.GetSliceFromKey(context, name, out loweredKey))
            {
                return Get(context, loweredKey);
            }
        }

        public FileSystemFile Get(FilesOperationContext context, Slice loweredKey)
        {
            var table = context.Transaction.InnerTransaction.OpenTable(FilesSchema, FilesMetadataSlice);

            TableValueReader tvr;
            if (table.ReadByKey(loweredKey, out tvr) == false)
                return null;

            var file = TableValueToFile(context, ref tvr);

            return file;
        }

        private static FileSystemFile TableValueToFile(FilesOperationContext context, ref TableValueReader tvr)
        {
            var result = new FileSystemFile
            {
                StorageId = tvr.Id
            };
            int size;
            // See format of the lazy string key in the GetLowerKeySliceAndStorageKey method
            byte offset;
            var ptr = tvr.Read(0, out size);
            result.LoweredKey = new LazyStringValue(null, ptr, size, context);

            ptr = tvr.Read(1, out size);
            result.Etag = Bits.SwapBytes(*(long*)ptr);

            ptr = tvr.Read(2, out size);
            size = BlittableJsonReaderBase.ReadVariableSizeInt(ptr, 0, out offset);
            result.Name = new LazyStringValue(null, ptr + offset, size, context);

            result.Metadata = new BlittableJsonReaderObject(tvr.Read(3, out size), size, context);

            // fetch the stream
            var tree = context.Transaction.InnerTransaction.ReadTree(FilesSlice);
            var streamIdentifier = tvr.Read(StreamIdentifierPosition, out size);
            Slice streamIdentifierSlice;
            using (Slice.External(context.Allocator, streamIdentifier, size, out streamIdentifierSlice))
            {
                result.Stream = tree.ReadStream(streamIdentifierSlice);
            }

            return result;
        }

        public long GenerateNextEtag()
        {
            return ++_lastEtag;
        }

        public long Put(FilesOperationContext context, string key, long? expectedEtag, Stream stream, BlittableJsonReaderObject metadata, 
            long? lastModifiedTicks = null)
        {
            if (context.Transaction == null)
            {
                ThrowRequiresTransaction();
                return default(long); // never reached
            }

            var newEtag = GenerateNextEtag();
            var newEtagBigEndian = Bits.SwapBytes(newEtag);

            var table = context.Transaction.InnerTransaction.OpenTable(FilesSchema, FilesMetadataSlice);

            byte* lowerKey;
            int lowerSize;
            byte* keyPtr;
            int keySize;
            DocumentKeyWorker.GetLowerKeySliceAndStorageKey(context, key, out lowerKey, out lowerSize, out keyPtr, out keySize);

            var modifiedTicks = lastModifiedTicks ?? _fileSystem.Time.GetUtcNow().Ticks;

            Slice keySlice;
            using (Slice.External(context.Allocator, lowerKey, (ushort) lowerSize, out keySlice))
            {
                TableValueReader oldValue;
                table.ReadByKey(keySlice, out oldValue);

                var transactionMarker = context.GetTransactionMarker();
                var tbv = new TableValueBuilder
                {
                    {lowerKey, lowerSize},
                    newEtagBigEndian,
                    {keyPtr, keySize},
                    {metadata.BasePointer, metadata.Size},
                    modifiedTicks,
                    transactionMarker
                };

                Slice streamIdentifierSlice;
                ByteStringContext<ByteStringMemoryCache>.Scope streamIdentifierScope;
                if (oldValue.Pointer == null)
                {
                    if (expectedEtag.HasValue && expectedEtag.Value != 0)
                    {
                        ThrowConcurrentExceptionOnMissingFile(key, expectedEtag.Value);
                    }

                    tbv.Add(newEtagBigEndian); // streamIdentifier
                    streamIdentifierScope = tbv.SliceFromLocation(context.Allocator, StreamIdentifierPosition, out streamIdentifierSlice);

                    table.Insert(tbv);
                }
                else
                {
                    int size;
                    var pOldEtag = oldValue.Read(1, out size);
                    var oldEtag = Bits.SwapBytes(*(long*) pOldEtag);

                    if (expectedEtag != null && oldEtag != expectedEtag)
                        ThrowConcurrentException(key, expectedEtag, oldEtag);

                    var streamIdentifier = oldValue.Read(StreamIdentifierPosition, out size);
                    tbv.Add(streamIdentifier, size);
                    streamIdentifierScope = Slice.External(context.Allocator, streamIdentifier, size, out streamIdentifierSlice);

                    table.Update(oldValue.Id, tbv);
                }

                using (streamIdentifierScope)
                {
                    var tree = context.Transaction.InnerTransaction.CreateTree(FilesSlice);
                    tree.AddStream(streamIdentifierSlice, stream);
                }

                _fileSystem.Metrics.FilePutsPerSecond.MarkSingleThreaded(1);
                // TODO: _fileSystem.Metrics.FileBytesPutsPerSecond.MarkSingleThreaded(streamSize);
            }

            return newEtag;
        }

        private static void ThrowRequiresTransaction([CallerMemberName]string caller = null)
        {
            // ReSharper disable once NotResolvedInText
            throw new ArgumentException("Context must be set with a valid transaction before calling " + caller, "context");
        }

        private static void ThrowConcurrentExceptionOnMissingFile(string key, long expectedEtag)
        {
            throw new ConcurrencyException(
                $"File {key} does not exists, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ExpectedETag = expectedEtag
            };
        }

        private static void ThrowConcurrentException(string key, long? expectedEtag, long oldEtag)
        {
            throw new ConcurrencyException(
                $"File {key} has etag {oldEtag}, but Put was called with etag {expectedEtag}. Optimistic concurrency violation, transaction will be aborted.")
            {
                ActualETag = oldEtag,
                ExpectedETag = expectedEtag ?? -1
            };
        }
    }
}