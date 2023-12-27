using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Sparrow;
using Voron;
using Voron.Data.BTrees;

namespace Corax.Indexing
{
    partial class IndexWriter
    {
#if CORAX_MEMORY_WATCHER
        private void CoraxInternalAllocationsCalculator(out long calculated)
        {
            //Method used to calculate internal allocations made by Corax.
            var totalAllocated = _entriesAllocator._totalAllocated + _transaction.LowLevelTransaction.Allocator._totalAllocated;
            var currentAllocated = _entriesAllocator._currentlyAllocated + _transaction.LowLevelTransaction.Allocator._currentlyAllocated;
            long nativeListsCurrentlyInUse = 0;
            long nativeListTotalAllocated = 0;
            long slicesSize = 0;
            foreach (var field in _fieldsMapping)
            {
                var fieldNativeListTotalAllocation = 0L;
                var fieldNativeListUsed = 0L;

                var indexedField = _knownFieldsTerms[field.FieldId];

                foreach (var key in indexedField.Textual.Keys)
                    slicesSize += key.Size;

                var addUsed = 0L;
                var addTotal = 0L;

                var remUsed = 0L;
                var remTotal = 0L;

                var upUsed = 0L;
                var upTotal = 0L;

                foreach (var termStore in indexedField.Storage)
                {
                    var addAllocations = termStore.Additions.Allocations;
                    addUsed += addAllocations.BytesUsed;
                    addTotal += addAllocations.BytesAllocated;

                    var remAllocations = termStore.Removals.Allocations;
                    remUsed += remAllocations.BytesUsed;
                    remTotal += remAllocations.BytesAllocated;


                    var upAllocations = termStore.Updates.Allocations;
                    upUsed += upAllocations.BytesUsed;
                    upTotal += upAllocations.BytesAllocated;

                    fieldNativeListTotalAllocation += addAllocations.BytesAllocated + remAllocations.BytesAllocated + upAllocations.BytesAllocated;
                    fieldNativeListUsed += addAllocations.BytesUsed + remAllocations.BytesUsed + upAllocations.BytesUsed;
                }

                Console.WriteLine(
                    $"{field.FieldName}: NativeLists: {new Size(fieldNativeListUsed, SizeUnit.Bytes)} / {new Size(fieldNativeListTotalAllocation, SizeUnit.Bytes)} | Add: {new Size(addUsed, SizeUnit.Bytes)} / {new Size(addTotal, SizeUnit.Bytes)}" +
                    $" | Rem: {new Size(remUsed, SizeUnit.Bytes)} / {new Size(remTotal, SizeUnit.Bytes)} | Up: {new Size(upUsed, SizeUnit.Bytes)} / {new Size(upTotal, SizeUnit.Bytes)} | Free space: {100 - (fieldNativeListUsed * 100) / (double)fieldNativeListTotalAllocation}%");
                nativeListTotalAllocated += fieldNativeListTotalAllocation;
                nativeListsCurrentlyInUse += fieldNativeListUsed;
            }

            var recordedTermTotalSize = 0L;
            var recordedTermTotalAllocated = 0L;
            foreach (var entry in _termsPerEntryId)
            {
                var stats = entry.Allocations;
                recordedTermTotalSize += stats.BytesUsed;
                recordedTermTotalAllocated += stats.BytesAllocated;
            }

            Console.WriteLine(
                $"Summary{Environment.NewLine}_entriesAllocator: {_entriesAllocator}{Environment.NewLine}LLT Allocator: {_transaction.LowLevelTransaction.Allocator}{Environment.NewLine}Transaction allocator: {_transaction.Allocator}");
            Console.WriteLine(
                $"Total term size: {new Size(slicesSize, SizeUnit.Bytes)}{Environment.NewLine}EntriesModification: {new Size(nativeListsCurrentlyInUse, SizeUnit.Bytes)} / {new Size(nativeListTotalAllocated, SizeUnit.Bytes)}  | Freespace: {100 - (nativeListsCurrentlyInUse * 100) / ((double)nativeListTotalAllocated)}%");
            Console.WriteLine($"Stored entries: {new Size(recordedTermTotalSize, SizeUnit.Bytes)} / {new Size(recordedTermTotalAllocated, SizeUnit.Bytes)}");
            calculated = nativeListsCurrentlyInUse + slicesSize + recordedTermTotalAllocated;
        }
#endif
        private readonly struct IndexOperationsDumper : IDisposable
        {
#if false
            private readonly FileStream _fs;
            private readonly BinaryWriter _bw;

            public IndexOperationsDumper(IndexFieldsMapping fieldsMapping)
            {
                _fs = File.OpenWrite("index.bin-log");
                _fs.Position = _fs.Length;
                _bw = new BinaryWriter(_fs);


                if (_fs.Length == 0)
                {
                    _bw.Write7BitEncodedInt(fieldsMapping.Count);
                    for (int i = 0; i < fieldsMapping.Count; i++)
                    {
                        IndexFieldBinding indexFieldBinding = fieldsMapping.GetByFieldId(i);
                        _bw.Write(indexFieldBinding.FieldName.ToString());
                    }
                }
            }

            public void Index(string id, Span<byte> data)
            {
                _bw.Write(id);
                _bw.Write7BitEncodedInt(data.Length);
                _bw.Write(data);
            }

            public void Commit()
            {
                _bw.Write("!Commit!");
            }

            public void Dispose()
            {
                _bw.Dispose();
            }
#else
            public IndexOperationsDumper(IndexFieldsMapping fieldsMapping)
            {
            }

            public void Commit()
            {
            }

            public void Dispose()
            {
            }
#endif
        }

        private struct IndexTermDumper : IDisposable
        {
#if ENABLE_TERMDUMPER
            private readonly StreamWriter _writer;

            public IndexTermDumper(Tree tree, Slice field)
            {
                _writer = File.AppendText(tree.Name.ToString() + field.ToString());
            }
#else
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IndexTermDumper(Tree tree, Slice field)
            {
            }
#endif

            [Conditional("ENABLE_TERMDUMPER")]
            public void WriteAddition(Slice term, long termId)
            {
#if ENABLE_TERMDUMPER
                _writer.WriteLine($"+ {term} {termId}");
#endif
            }

            [Conditional("ENABLE_TERMDUMPER")]
            public void WriteRemoval(Slice term, long termId)
            {
#if ENABLE_TERMDUMPER
                _writer.WriteLine($"- {term} {termId}");
#endif
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {
#if ENABLE_TERMDUMPER
                _writer?.Dispose();
#endif
            }
        }
    }
}
