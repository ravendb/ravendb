using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Voron;
using Voron.Data.BTrees;

namespace Corax
{
    partial class IndexWriter
    {
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
