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

            //new SimplePipelineTest(new ConsoleTestOutputHelper()).BasicInnerAnalyzer();


            //var parser = new QueryParser();
            //parser.Init("from Dogs where Type = 'Dog' and Age in ('15', '16')");
            //QueryDefinition queryDefinition = new QueryDefinition("Name", parser.Parse());

            //using var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly());

            //GenerateData(env);

            //using var searcher = new IndexSearcher(env);
            //var coraxQueryEvaluator = new CoraxQueryEvaluator(searcher);

            //var watch = Stopwatch.StartNew();

            //Span<long> ids = stackalloc long[2048];
            //for ( int i = 0; i < 1000; i++)
            //{
            //    var typeTerm = searcher.TermQuery("Type", "Dog");
            //    var ageTerm = searcher.StartWithQuery("Age", "1");
            //    var andQuery = searcher.And(typeTerm, ageTerm);
            //    var query = searcher.OrderByAscending(andQuery, fieldId: 2, take: 16);

            //    while (query.Fill(ids) != 0)
            //        ;
            //}

            //watch.Stop();
            //Console.WriteLine($"Cost: {watch.ElapsedMilliseconds} ms.");
            //Console.WriteLine($"Cost Per Search: {watch.ElapsedMilliseconds / 1000} ms.");


            //var query = searcher.Search(queryDefinition.Query.Where);

            //Span<long> ids = stackalloc long[2048];
            //int read;
            //do
            //{
            //    read = query.Fill(ids);
            //    for (int i = 0; i < read; i++)
            //        Console.WriteLine(searcher.GetIdentityFor(ids[i]));
            //}
            //while (read != 0);

            //new IndexSearcherTest(new ConsoleTestOutputHelper()).SimpleAndOr();

            //using (var writer = new IndexWriter(env))
            //{
            //    using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            //    Slice.From(bsc, "Name", ByteStringType.Immutable, out var nameSlice);
            //    Slice.From(bsc, "Family", ByteStringType.Immutable, out var familySlice);
            //    Slice.From(bsc, "Age", ByteStringType.Immutable, out var ageSlice);
            //    Slice.From(bsc, "Type", ByteStringType.Immutable, out var typeSlice);

            //    Span<byte> buffer = new byte[256];
            //    var fields = new Dictionary<Slice,int>
            //    {
            //        [nameSlice] = 0,
            //        [familySlice] = 1,
            //        [ageSlice] = 2,
            //        [typeSlice] = 3
            //    };

            //    {
            //        var entryWriter = new IndexEntryWriter(buffer, fields);
            //        entryWriter.Write(0, Encoding.UTF8.GetBytes("Arava"));
            //        entryWriter.Write(1, Encoding.UTF8.GetBytes("Eini"));
            //        entryWriter.Write(2, BitConverter.GetBytes(12), 12L, 12D);
            //        entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
            //        entryWriter.Finish(out var entry);

            //        writer.Index("dogs/arava", entry, fields);
            //    }

            //    {
            //        var entryWriter = new IndexEntryWriter(buffer, fields);
            //        entryWriter.Write(0, Encoding.UTF8.GetBytes("Phoebe"));
            //        entryWriter.Write(1, Encoding.UTF8.GetBytes("Eini"));
            //        entryWriter.Write(2, BitConverter.GetBytes(7), 7L, 7D);
            //        entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
            //        entryWriter.Finish(out var entry);

            //        writer.Index("dogs/phoebe", entry, fields);
            //    }

            //    for (int i = 0; i < 10_000; i++)
            //    {
            //        var entryWriter = new IndexEntryWriter(buffer, fields);
            //        entryWriter.Write(0, Encoding.UTF8.GetBytes("Dog #" + i));
            //        entryWriter.Write(1, Encoding.UTF8.GetBytes("families/" + (i % 1024)));
            //        var age = i % 17;
            //        entryWriter.Write(2, BitConverter.GetBytes(age), age, age);
            //        entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
            //        entryWriter.Finish(out var entry);

            //        writer.Index("dogs/" + i, entry, fields);
            //    }

            //    writer.Commit();
            //}


            //using (var searcher = new IndexSearcher(env))
            //{
            //    var termMatch = searcher.Search("from Dogs where Type = 'Dog' and Family = 'Eini'");
            //    PrintIds(termMatch, searcher);
            //}

        }

        private static void GenerateData(StorageEnvironment env)
        {
            using (var writer = new IndexWriter(env))
            {
                using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
                Slice.From(bsc, "Name", ByteStringType.Immutable, out var nameSlice);
                Slice.From(bsc, "Family", ByteStringType.Immutable, out var familySlice);
                Slice.From(bsc, "Age", ByteStringType.Immutable, out var ageSlice);
                Slice.From(bsc, "Type", ByteStringType.Immutable, out var typeSlice);

                Span<byte> buffer = new byte[256];

                var fields = new IndexFieldsMapping(bsc)
                                    .AddBinding(0, nameSlice)
                                    .AddBinding(1, familySlice)
                                    .AddBinding(2, ageSlice)
                                    .AddBinding(3, typeSlice);

                {
                    var entryWriter = new IndexEntryWriter(buffer, fields);
                    entryWriter.Write(0, Encoding.UTF8.GetBytes("Arava"));
                    entryWriter.Write(1, Encoding.UTF8.GetBytes("Eini"));
                    //entryWriter.Write(2, BitConverter.GetBytes(12), 12L, 12D);
                    entryWriter.Write(2, Encoding.UTF8.GetBytes(12L.ToString()), 12L, 12D);
                    entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    entryWriter.Finish(out var entry);

                    writer.Index("dogs/arava", entry, fields);
                }

                {
                    var entryWriter = new IndexEntryWriter(buffer, fields);
                    entryWriter.Write(0, Encoding.UTF8.GetBytes("Phoebe"));
                    entryWriter.Write(1, Encoding.UTF8.GetBytes("Eini"));
                    //entryWriter.Write(2, BitConverter.GetBytes(7), 7L, 7D);
                    entryWriter.Write(2, Encoding.UTF8.GetBytes(7.ToString()), 7L, 7D);
                    entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    entryWriter.Finish(out var entry);

                    writer.Index("dogs/phoebe", entry, fields);
                }

                for (int i = 0; i < 100_000; i++)
                {
                    var entryWriter = new IndexEntryWriter(buffer, fields);
                    entryWriter.Write(0, Encoding.UTF8.GetBytes("Dog #" + i));
                    entryWriter.Write(1, Encoding.UTF8.GetBytes("families/" + (i % 1024)));
                    var age = i % 17;
                    //entryWriter.Write(2, BitConverter.GetBytes(age), age, age);
                    entryWriter.Write(2, Encoding.UTF8.GetBytes(age.ToString()), age, age);
                    entryWriter.Write(3, Encoding.UTF8.GetBytes("Dog"));
                    entryWriter.Finish(out var entry);

                    writer.Index("dogs/" + i, entry, fields);
                }

                writer.Commit();
            }
        }

        private static void PrintIds(IQueryMatch termMatch, IndexSearcher searcher)
        {
            Span<long> ids = stackalloc long[128];

            var list = new List<string>();            
            int read;
            do
            {
                read = termMatch.Fill(ids);
                for (int i = 0; i < read; i++)
                    list.Add(searcher.GetIdentityFor(ids[i]));
            }
            while (read != 0);

            Console.WriteLine(list.Count + " results");
            Console.WriteLine(string.Join(", ", list.Take(16)));
        }
    }
}
