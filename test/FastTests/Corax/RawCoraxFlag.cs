using System;
using System.Collections.Generic;
using Corax;
using Corax.Mappings;
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
    public unsafe void CanStoreBlittableWithWriterScopeDynamicField()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        _analyzers = CreateKnownFields(_bsc, true);
        using var blittable1 = ctx.ReadObject(json1, "foo");
        using var blittable2 = ctx.ReadObject(json2, "foo");
        {
            
            using var writer = new IndexWriter(Env, _analyzers);
            var entry = new IndexEntryWriter(bsc, _analyzers);

            foreach (var (id, item) in new[] {("1", blittable1), ("2", blittable2)})
            {                
                entry.Write(IndexId, Encodings.Utf8.GetBytes(id));
                using (var scope = new BlittableWriterScope(item))
                {
                    scope.Write("Dynamic", Constants.IndexWriter.DynamicField, ref entry);
                }

                using var _ = entry.Finish(out var output);                
                writer.Index(id,output.ToSpan());
            }

            writer.PrepareAndCommit();
        }

        using var __ = Slice.From(bsc, "Dynamic", out var fieldName);
        
        Span<long> mem = stackalloc long[1024];
        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            ;
            var match = searcher.SearchQuery(_analyzers.GetByFieldId(0).Metadata, new List<string>(){"1"}, Constants.Search.Operator.Or);
            Assert.Equal(1, match.Fill(mem));
            var result = searcher.GetEntryReaderFor(mem[0]);
            result.GetFieldReaderFor(fieldName).Read(out Span<byte> blittableBinary);

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
            Assert.True(indexWriter.TryDeleteEntry("1"));
            indexWriter.PrepareAndCommit();
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(1, match.Fill(mem));
            long id = mem[0];
            Assert.Equal("2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
        }
        
    }

    [Fact]
    public unsafe void CanStoreBlittableWithWriterScope()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        _analyzers = CreateKnownFields(_bsc, true);
        using var blittable1 = ctx.ReadObject(json1, "foo");
        using var blittable2 = ctx.ReadObject(json2, "foo");
        {
            
            using var writer = new IndexWriter(Env, _analyzers);
            var entry = new IndexEntryWriter(bsc, _analyzers);

            foreach (var (id, item) in new[] {("1", blittable1), ("2", blittable2)})
            {                
                entry.Write(IndexId, Encodings.Utf8.GetBytes(id));
                using (var scope = new BlittableWriterScope(item))
                {
                    scope.Write(string.Empty, ContentId, ref entry);
                }

                using var _ = entry.Finish(out var output);                
                writer.Index(id, output.ToSpan());
            }

            writer.PrepareAndCommit();
        }

        Span<long> mem = stackalloc long[1024];
        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.SearchQuery(_analyzers.GetByFieldId(0).Metadata, new List<string>(){"1"}, Constants.Search.Operator.Or);
            Assert.Equal(1, match.Fill(mem));
            var result = searcher.GetEntryReaderFor(mem[0]);
            result.GetFieldReaderFor(1).Read(out Span<byte> blittableBinary);

            fixed (byte* ptr = &blittableBinary.GetPinnableReference())
            {
                var reader = new BlittableJsonReaderObject(ptr, blittableBinary.Length, ctx);
                Assert.Equal(blittable1.Size, reader.Size);
                Assert.True(blittable1.Equals(reader));
            }
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(2, match.Fill(mem));
        }
        
        //Delete part
        using (var indexWriter = new IndexWriter(Env, _analyzers))
        {
            Assert.True(indexWriter.TryDeleteEntry("1"));
            indexWriter.PrepareAndCommit();
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(1, match.Fill(mem));
            long id = mem[0];
            Assert.Equal("2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
        }
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, bool analyzers)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using var builder = (analyzers
            ? IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice, Analyzer.CreateDefaultAnalyzer(ctx))
                .AddBinding(ContentId, contentSlice, LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30)))
            : IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice, fieldIndexingMode: FieldIndexingMode.No));
        
        return builder.Build();
    }

    public override void Dispose()
    {
        _bsc.Dispose();
        _analyzers.Dispose();
        base.Dispose();
    }
}
