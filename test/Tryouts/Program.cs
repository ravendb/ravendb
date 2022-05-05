using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Corax;
using FastTests.Corax;
using FastTests.Sparrow;
using FastTests.Voron;
using FastTests.Voron.Sets;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Sets;
using Raven.Server.Documents.Queries.Parser;
using Corax.Queries;
using NuGet.Packaging.Signing;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        [StructLayout(LayoutKind.Sequential)]
        public struct IndexValue
        {
            public int A;
            public int B;
            public int C;
            public int D;
        }

        public class DocumentExample
        {
            public int A;
            public string B;
            public bool C;
            public long D;
        }


        //public static void SyncMain()
        //{
        //    var template = new IndexEntryTemplate<IndexValue>();

        //    Span<byte> buffer = new byte[1000];
        //    var documents = new DocumentExample[0];
        //    using var _ = StorageEnvironment.GetStaticContext(out var ctx);
        //    Slice.From(ctx, "Dogs", ByteStringType.Immutable, out Slice docsSlice); 

        //    // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
        //    var aField = template.GetField(x => x.A);
        //    var bField = template.GetField(x => x.B);
        //    var cField = template.GetField(x => x.C);
        //    var dField = template.GetField(x => x.D);
        //    var dynamicDogsField = template.GetDynamicField(docsSlice, IndexEntryFieldType.Int64);
        //    foreach (var document in documents)
        //    {
        //        var writer = template.CreateWriter(buffer);
        //        writer.Write(aField, document.A);
        //        writer.Write(bField, document.B);
        //        writer.Write(cField, document.C);
        //        writer.Write(dField, document.D);
        //        writer.Write(dynamicDogsField, 10);
        //        var length = writer.Finish();
        //    }
        //}

        private readonly struct StringArrayIterator : IReadOnlySpanIndexer
        {
            private readonly string[] _values;

            public StringArrayIterator(string[] values)
            {
                _values = values;
            }

            public int Length => _values.Length;

            public bool IsNull(int i)
            {
                if (i < 0 || i >= Length)
                    throw new ArgumentOutOfRangeException();

                return _values[i] == null;
            }
            
            public ReadOnlySpan<byte> this[int i] => Encoding.UTF8.GetBytes(_values[i]);
        }

        public static void SyncMain()
        {
            Span<byte> buffer = new byte[1000];
            var documents = new DocumentExample[1];
            using var _ = StorageEnvironment.GetStaticContext(out var ctx);
            Slice.From(ctx, "A", ByteStringType.Immutable, out Slice aSlice);
            Slice.From(ctx, "B", ByteStringType.Immutable, out Slice bSlice);
            Slice.From(ctx, "C", ByteStringType.Immutable, out Slice cSlice);
            Slice.From(ctx, "D", ByteStringType.Immutable, out Slice dSlice);

            // The idea is that GetField will return an struct we can use later on a loop (we just get it once).
            var knownFields = new IndexFieldsMapping(ctx)
                                    .AddBinding(0, aSlice)
                                    .AddBinding(1, bSlice)
                                    .AddBinding(2, cSlice)
                                    .AddBinding(3, dSlice);

            var writer = new IndexEntryWriter(buffer, knownFields);
            writer.Write(0, Encoding.UTF8.GetBytes("1.001"), 1, 1.001);
            writer.Write(1, new StringArrayIterator(new []{"AAA", "BF", "CE"}));
            writer.Write(2, Encoding.UTF8.GetBytes("CCCC"));
            writer.Write(3, Encoding.UTF8.GetBytes("DDDDDDDDDD"));
            var length = writer.Finish(out var element);

            var reader = new IndexEntryReader(element);
            reader.Read(0, out long longValue);
            reader.Read(0, out int intValue);
            reader.Read(0, out double doubleValue);
            reader.Read(0, out double floatValue);
            reader.Read(0, out longValue, out doubleValue, out var sequenceValue);
            reader.Read(2, value: out sequenceValue);
            reader.Read(3, value: out sequenceValue);

            reader.Read(1, value: out sequenceValue, elementIdx: 0);
            reader.Read(1, value: out sequenceValue, elementIdx: 2);

            var iterator = reader.ReadMany(1);
            while (iterator.ReadNext())
            {
            }
        }

        public static void Main()
        {
            new SimplePipelineTest(new ConsoleTestOutputHelper()).BasicAnalyzer();
        }
    }
}
