using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Sparrow;
using Tryouts.Corax.Analyzers;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Tryouts.Corax
{
    public class FullTextIndex : IDisposable
    {
        private readonly StorageEnvironment _env;

        private readonly TableSchema _entriesSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                // TODO: this is standard b+tree, we probably want to use a fixed size tree here
                StartIndex = 0,
                Count = 1
            });

        private readonly UnmanagedBuffersPool _pool;
        private DefaultAnalyzer _analyzer;

        public FullTextIndex(StorageEnvironmentOptions options)
        {
            try
            {
                _pool = new UnmanagedBuffersPool("Index for " + options.BasePath);
                _env = new StorageEnvironment(options);
                _analyzer = new DefaultAnalyzer();
                using (var tx = _env.WriteTransaction())
                {
                    tx.CreateTree("Fields");
                    tx.CreateTree("Options");
                    _entriesSchema.Create(tx, "IndexEntries");
                    tx.Commit();
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        public Indexer CreateIndexer()
        {
            return new Indexer(this);
        }


        public Searcher CreateSearcher()
        {
            return new Searcher(this);
        }

        public class Searcher : IDisposable
        {
            private readonly FullTextIndex _parent;
            private readonly Transaction _tx;
            private readonly RavenOperationContext _context;

            public Searcher(FullTextIndex parent)
            {
                _parent = parent;
                _tx = _parent._env.ReadTransaction();
                _context = new RavenOperationContext(_parent._pool);
            }

            public unsafe string[] Query(string name, string value)
            {
                var property = _tx.ReadTree(name);
                if (property == null)
                    return Array.Empty<string>();

                var fixedSizeTree = new FixedSizeTree(_tx.LowLevelTransaction, property, value, 0);
                if (fixedSizeTree.NumberOfEntries == 0)
                    return Array.Empty<string>();

                var docs = new string[fixedSizeTree.NumberOfEntries];
                var entries = new Table(_parent._entriesSchema, "IndexEntries", _tx);

                long index = 0;
                long entryId = 0L;
                var key = new Slice((byte*)&entryId, sizeof(long));
                using (var it = fixedSizeTree.Iterate())
                {
                    entryId = IPAddress.HostToNetworkOrder(it.CurrentKey);

                    var tvr = entries.ReadByKey(key);
                    int size;
                    var ptr = tvr.Read(1, out size);
                    var entry = new BlittableJsonReaderObject(ptr, size, _context);
                    string docId;
                    entry.TryGet(Constants.DocumentIdFieldName, out docId);
                    docs[index] = docId;
                }

                return docs;
            }

            public void Dispose()
            {
                _tx?.Dispose();
                _context?.Dispose();
            }
        }

        public class Indexer : IDisposable
        {
            private readonly FullTextIndex _parent;
            private readonly RavenOperationContext _context;
            private readonly int _batchSize;
            private readonly List<BlittableJsonReaderObject> _newEntries = new List<BlittableJsonReaderObject>();
            private readonly List<string> _deletes = new List<string>();
            private long _size;
            private ITokenSource _tokenSource;
            private MmapStream _mmapStream;
            private StreamReader _mmapReader;


            public unsafe Indexer(FullTextIndex parent, int batchSize = 1024 * 1024 * 16)
            {
                _parent = parent;
                _batchSize = batchSize;
                _context = new RavenOperationContext(_parent._pool);

                _mmapStream = new MmapStream(null, 0);
                _mmapReader = new StreamReader(_mmapStream);
            }

            public void Delete(string identifier)
            {
                _deletes.Add(identifier);
            }

            public void NewEntry(DynamicJsonValue entry, string identifier)
            {
                entry[Constants.DocumentIdFieldName] = identifier;
                var values = new List<LazyStringValue>();
                var analyzedEntry = new DynamicJsonValue();
                foreach (var property in entry.Properties)
                {
                    var field = property.Item1;
                    var value = GetReaderForValue(property.Item2);

                    _tokenSource = _parent._analyzer.CreateTokenSource(field, _tokenSource);
                    _tokenSource.SetReader(value);
                    values.Clear();
                    while (_tokenSource.Next())
                    {
                        if (_parent._analyzer.Process(field, _tokenSource) == false)
                            continue;

                        values.Add(_context.GetLazyString(_tokenSource.Buffer, 0, _tokenSource.Size));
                    }
                    if (values.Count > 1)
                    {
                        var dynamicJsonArray = new DynamicJsonArray();
                        foreach (var val in values)
                        {
                            dynamicJsonArray.Add(val);
                        }
                        analyzedEntry[field] = dynamicJsonArray;
                    }
                    else
                    {
                        analyzedEntry[field] = values[0];
                    }
                }
                var blitEntry =  _context.ReadObject(analyzedEntry, identifier);
                _newEntries.Add(blitEntry);
                _size += blitEntry.Size;
                if (_size < _batchSize)
                    return;
                Flush();
            }

            private unsafe TextReader GetReaderForValue(object item2)
            {
                var s = item2 as string;
                if (s != null)
                    return new StringReader(s);
                var lsv = item2 as LazyStringValue;
                if (lsv != null)
                {
                    _mmapStream.Set(lsv.Buffer, lsv.Size);
                    _mmapReader.DiscardBufferedData();
                    return _mmapReader;
                }

                throw new NotSupportedException("Cannot process " + item2);
            }

            private unsafe class MmapStream : Stream
            {
                private  byte* ptr;
                private  long len;
                private long pos;

                public MmapStream(byte* ptr, long len)
                {
                    this.ptr = ptr;
                    this.len = len;
                }

                public override void Flush()
                {
                }

                public override long Seek(long offset, SeekOrigin origin)
                {
                    switch (origin)
                    {
                        case SeekOrigin.Begin:
                            Position = offset;
                            break;
                        case SeekOrigin.Current:
                            Position += offset;
                            break;
                        case SeekOrigin.End:
                            Position = len + offset;
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("origin", origin, null);
                    }
                    return Position;
                }

                public override void SetLength(long value)
                {
                    throw new NotSupportedException();
                }

                public override int ReadByte()
                {
                    if (Position == len)
                        return -1;
                    return ptr[pos++];
                }

                public override int Read(byte[] buffer, int offset, int count)
                {
                    if (pos == len)
                        return 0;
                    if (count > len - pos)
                    {
                        count = (int) (len - pos);
                    }
                    fixed (byte* dst = buffer)
                    {
                        Memory.CopyInline(dst + offset, ptr + pos, count);
                    }
                    pos += count;
                    return count;
                }

                public override void Write(byte[] buffer, int offset, int count)
                {
                    throw new NotSupportedException();
                }

                public override bool CanRead => true;

                public override bool CanSeek => true;

                public override bool CanWrite => false;

                public override long Length => len;
                public override long Position { get { return pos; } set { pos = value; } }

                public void Set(byte* buffer, int size)
                {
                    this.ptr = buffer;
                    this.len = size;
                    pos = 0;
                }
            }


            private unsafe void Flush()
            {
                if (_newEntries.Count == 0 && _deletes.Count == 0)
                    return;

                using (var tx = _parent._env.WriteTransaction())
                {
                    var entries = new Table(_parent._entriesSchema, "IndexEntries", tx);

                    var identifiersTree = tx.CreateTree(Constants.DocumentIdFieldName);

                    foreach (var identifier in _deletes)
                    {
                        DeleteEntry(tx, entries, identifiersTree, identifier);
                    }

                    var options = tx.CreateTree("Options");

                    var readResult = options.Read("LastEntryId");
                    long lastEntryId = 1;
                    if (readResult != null)
                        lastEntryId = readResult.Reader.ReadLittleEndianInt64();

                    foreach (var entry in _newEntries)
                    {
                        AddEntry(tx, entries, entry, lastEntryId++);
                        entry.Dispose();
                    }
                    options.Add("LastEntryId", new Slice((byte*)&lastEntryId, sizeof(long)));
                    tx.Commit();
                }
                _size = 0;
                _newEntries.Clear();
            }

            private unsafe void DeleteEntry(Transaction tx, Table entries, Tree identifiersTree, string identifier)
            {
                // this should have just a single item in it
                var fixedSizeTree = new FixedSizeTree(tx.LowLevelTransaction, identifiersTree, identifier, 0);
                using (var it = fixedSizeTree.Iterate())
                {
                    if (it.Seek(long.MinValue) == false)
                        return;

                    var entryId = it.CurrentKey;
                    var bigEndianEntryId = IPAddress.HostToNetworkOrder(entryId);

                    var tvr = entries.ReadByKey(new Slice((byte*)&bigEndianEntryId, sizeof(long)));

                    int size;
                    var entry = new BlittableJsonReaderObject(tvr.Read(1, out size), size, _context);

                    for (int i = 0; i < entry.Count; i++)
                    {
                        var propertyByIndex = entry.GetPropertyByIndex(i);
                        var property = propertyByIndex.Item1;

                        //TODO: implement this without the field allocations
                        //var slice = new Slice(lazyStringValue.Buffer, (u short)lazyStringValue.Size);
                        var fieldTree = tx.CreateTree(property.ToString());

                        //TODO: right now only supporting strings
                        var value = (LazyStringValue)propertyByIndex.Item2;

                        var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree,
                            new Slice(value.Buffer, (ushort)value.Size), 0);
                        fst.Delete(entryId);
                    }

                    entries.Delete(tvr.Id);
                }
            }

            private unsafe void AddEntry(Transaction tx, Table entries, BlittableJsonReaderObject entry, long entryId)
            {
                long bigEndianId = IPAddress.HostToNetworkOrder(entryId);
                entries.Set(new TableValueBuilder
                {
                    {(byte*)&bigEndianId, sizeof(long)},
                    {entry.BasePointer, entry.Size}
                });

                //for (int i = 0; i < entry.Count; i++)
                //{
                //    var propertyByIndex = entry.GetPropertyByIndex(i);
                //    var property = propertyByIndex.Item1;

                //    if (property.Size > byte.MaxValue)
                //        throw new InvalidOperationException("Field name cannot exceed 255 bytes");

                //    //TODO: implement this without the field allocations
                //    //var slice = new Slice(lazyStringValue.Buffer, (u short)lazyStringValue.Size);
                //    var fieldTree = tx.CreateTree(property.ToString());

                //    //TODO: right now only supporting strings
                //    var value = (LazyStringValue)propertyByIndex.Item2;
                //    if (value.Size > byte.MaxValue)
                //        throw new InvalidOperationException("Field value cannot exceed 255 bytes");

                //    var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree,
                //        new Slice(value.Buffer, (ushort)value.Size), 0);
                //    fst.Add(entryId);
                //}
            }


            public void Dispose()
            {
                try
                {
                    Flush();
                }
                finally
                {
                    _context?.Dispose();
                }
            }
        }

        public void Dispose()
        {
            _pool?.Dispose();
            _env?.Dispose();
        }
    }
}