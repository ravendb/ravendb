using System;
using Corax;
using Corax.Analyzers;
using Corax.IndexEntry;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.WriterScopes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RawCoraxFlag : StorageTest
{
    private const int IndexId = 0, ContentId = 1;
    private IndexFieldsMapping _analyzers;
    private readonly ByteStringContext _bsc;
    private DynamicJsonValue json1 = new() {["Address"] = new DynamicJsonValue() {["City"] = "Atlanta", ["ZipCode"] = 1254}, ["Friends"] = "yes"};
    private DynamicJsonValue json2 = new() {["Address"] = new DynamicJsonValue() {["ZipCode"] = 1234, ["City"] = "New York"}, ["Friends"] = "no"};

    public RawCoraxFlag(ITestOutputHelper output) : base(output)
    {
        _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
    }

    [Fact]
    public unsafe void CanStoreBlittableWithWriterScope()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        _analyzers = CreateKnownFields(_bsc, true);
        using var blittable1 = ctx.ReadObject(json1, "foo");
        using var blittable2 = ctx.ReadObject(json2, "foo");
        {
            Span<byte> buffer = new byte[10 * 1024];
            using var writer = new IndexWriter(Env, _analyzers);
            foreach (var (id, item) in new[] {("1", blittable1), ("2", blittable2)})
            {
                var entry = new IndexEntryWriter(buffer, _analyzers);
                entry.Write(IndexId, Encodings.Utf8.GetBytes(id));
                using (var scope = new BlittableWriterScope(item))
                {
                    scope.Write(ContentId, ref entry);
                }

                entry.Finish(out var output);
                writer.Index(id, output);
            }

            writer.Commit();
        }

        Span<long> mem = stackalloc long[1024];
        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.SearchQuery("Id", "1", Constants.Search.Operator.Or, false, IndexId);
            Assert.Equal(1, match.Fill(mem));
            var result = searcher.GetReaderFor(mem[0]);
            result.Read(1, out Span<byte> blittableBinary);

            fixed (byte* ptr = &blittableBinary.GetPinnableReference())
            {
                var reader = new BlittableJsonReaderObject(ptr, blittableBinary.Length, ctx);
                Assert.Equal(blittable1.Size, reader.Size);
                Assert.True(blittable1.Equals(reader));
            }
        }

        //Delete part
        using (var indexWriter = new IndexWriter(Env, _analyzers))
        {
            Assert.True(indexWriter.TryDeleteEntry("Id", "1"));
            indexWriter.Commit();
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(1, match.Fill(mem));
            Assert.Equal("2", searcher.GetIdentityFor(mem[0]));
        }
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, bool analyzers)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        return analyzers
            ? new IndexFieldsMapping(ctx)
                .AddBinding(IndexId, idSlice, Analyzer.DefaultAnalyzer)
                .AddBinding(ContentId, contentSlice, LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30)))
            : new IndexFieldsMapping(ctx)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice, fieldIndexingMode: FieldIndexingMode.No);
    }

    public override void Dispose()
    {
        _bsc.Dispose();
        base.Dispose();
    }
}
