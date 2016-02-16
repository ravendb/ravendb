using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Indexing.Corax
{
    public class Indexer : IDisposable
    {
        private readonly FullTextIndex _parent;
        private readonly MemoryOperationContext _context;
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
            _context = new MemoryOperationContext(_parent.Pool);
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

                _tokenSource = _parent.Analyzer.CreateTokenSource(field, _tokenSource);
                _tokenSource.SetReader(value);
                values.Clear();
                while (_tokenSource.Next())
                {
                    if (_parent.Analyzer.Process(field, _tokenSource) == false)
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
            var blitEntry = _context.ReadObject(analyzedEntry, identifier);
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



        private unsafe void Flush()
        {
            if (_newEntries.Count == 0 && _deletes.Count == 0)
                return;

            using (var tx = _parent.Env.WriteTransaction())
            {
                var entries = new Table(_parent.EntriesSchema, "IndexEntries", tx);

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
                    var fieldTree = tx.CreateTree(property.ToString());

                    foreach (var slice in GetValuesFor(propertyByIndex.Item2))
                    {
                        var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree, slice, 0);
                        fst.Delete(entryId);
                    }
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

            for (int i = 0; i < entry.Count; i++)
            {
                var propertyByIndex = entry.GetPropertyByIndex(i);
                var property = propertyByIndex.Item1;

                if (property.Size > byte.MaxValue)
                    throw new InvalidOperationException("Field name cannot exceed 255 bytes");

                //TODO: implement this without the field allocations
                //var slice = new Slice(lazyStringValue.Buffer, (u short)lazyStringValue.Size);
                var fieldTree = tx.CreateTree(property.ToString());

                foreach (var slice in GetValuesFor(propertyByIndex.Item2))
                {
                    var fst = new FixedSizeTree(tx.LowLevelTransaction, fieldTree, slice, 0);
                    fst.Add(entryId);
                }
            }
        }

        private unsafe Slice[] GetValuesFor(object obj)
        {
            //TODO: right now only supporting strings
            var stringValue = obj as LazyStringValue;
            if (stringValue != null)
            {
                var value = stringValue;
                if (value.Size > byte.MaxValue)
                    throw new InvalidOperationException("Field value cannot exceed 255 bytes");

                var valueSlice = new Slice(value.Buffer, (ushort)value.Size);
                return new[] { valueSlice };
            }
            var csv = obj as LazyCompressedStringValue;
            if (csv != null)
                return GetValuesFor(csv.ToLazyStringValue());

            var array = obj as BlittableJsonReaderArray;
            if (array != null)
            {
                var list = new List<Slice>(array.Length);
                for (int i = 0; i < array.Length; i++)
                {
                    list.AddRange(GetValuesFor(array[i]));
                }
                return list.ToArray();
            }

            throw new InvalidOperationException("Don't know (yet?) how to index " + obj);
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
}