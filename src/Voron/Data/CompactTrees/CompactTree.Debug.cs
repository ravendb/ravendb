using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.VisualBasic;
using Sparrow;
using Voron.Exceptions;

namespace Voron.Data.CompactTrees;

unsafe partial class CompactTree
{
    private static class CompactTreeDumper
    {
        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteCommit(CompactTree tree)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine("###");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteAddition(CompactTree tree, ReadOnlySpan<byte> key, long value)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine($"+{Encodings.Utf8.GetString(key)}|{value}");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteRemoval(CompactTree tree, ReadOnlySpan<byte> key)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine($"-{Encodings.Utf8.GetString(key)}");
#endif
        }
    }


    public void VerifyOrderOfElements()
    {
        var it = Iterate();
        it.Reset();

        var prevKeyStorage = new byte[4096];
        Span<byte> prevKey = prevKeyStorage.AsSpan();
        while (it.MoveNext(out var compactKey, out var v))
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
}
