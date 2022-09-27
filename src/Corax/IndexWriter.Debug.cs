using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using Voron;
using Voron.Data.BTrees;

namespace Corax
{
    partial class IndexWriter
    {
        private struct IndexTermDumper : IDisposable
        {
#if ENABLE_TERMDUMPER
            private readonly StreamWriter _writer;

            public IndexTermDumper(Tree tree, Slice field)
            {
                _writer = File.AppendText(tree.Name.ToString() + fieldId);
            }
#else
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public IndexTermDumper(Tree tree, Slice field)
            {}
#endif

            [Conditional("ENABLE_TERMDUMPER")]
            public void WriteBatch()
            {
#if ENABLE_TERMDUMPER
                _writer.WriteLine("###");
#endif
            }

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
