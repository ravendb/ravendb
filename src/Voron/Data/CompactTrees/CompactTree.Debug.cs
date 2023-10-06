using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using Sparrow;

namespace Voron.Data.CompactTrees;

unsafe partial class CompactTree
{
    private static class CompactTreeDumper
    {
        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteCommit(CompactTree tree)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree._inner.Name.ToString());
            writer.WriteLine("###");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteAddition(CompactTree tree, ref CompactKeyLookup lookup, long value)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree._inner.Name.ToString());
            writer.WriteLine($"+{Encodings.Utf8.GetString(lookup.Key.Decoded())}|{value}");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteBulkSet(CompactTree tree, ref CompactKeyLookup lookup, long value)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree._inner.Name.ToString());
            writer.WriteLine($"*{Encodings.Utf8.GetString(lookup.Key.Decoded())}|{value}");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteRemoval(CompactTree tree, ref CompactKeyLookup lookup, long oldValue)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree._inner.Name.ToString());
            writer.WriteLine($"-{Encodings.Utf8.GetString(lookup.Key.Decoded())}|{oldValue}");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteBulkRemoval(CompactTree tree, ref CompactKeyLookup lookup, long oldValue)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree._inner.Name.ToString());
            writer.WriteLine($"/{Encodings.Utf8.GetString(lookup.Key.Decoded())}|{oldValue}");
#endif
        }
    }


    public void VerifyOrderOfElements()
    {
        _inner.VerifyStructure();
        
        var it = Iterate();
        it.Reset();

        var prevKeyStorage = new byte[4096];
        Span<byte> prevKey = prevKeyStorage.AsSpan();
        while (it.MoveNext(out var compactKey, out var v, out _))
        {
            var key = compactKey.Decoded();
            if (prevKey.SequenceCompareTo(key) > 0)
            {
                throw new InvalidDataException("The items in the compact tree are not sorted!");
            }

            // We copy the current key to the storage and update.
            prevKey = prevKeyStorage.AsSpan();
            key.CopyTo(prevKey);
            prevKey = prevKey.Slice(0, key.Length);
            compactKey.Dispose();
        }
    }
    
    public void Render()
    {
        _inner.Render();
    }
}
