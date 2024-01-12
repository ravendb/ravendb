using System;
using System.Collections.Generic;
using Corax;
using Corax.Analyzers;
using Corax.Querying;
using Corax.Mappings;
using Corax.Pipeline;
using FastTests.Voron;
using NetTopologySuite.Utilities;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;
using Assert = Xunit.Assert;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

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
        Slice.From(bsc, "Dynamic", ByteStringType.Immutable, out Slice dynamicSlice);

        _analyzers = CreateKnownFields(_bsc, true);
        using var blittable1 = ctx.ReadObject(json1, "foo");
        using var blittable2 = ctx.ReadObject(json2, "foo");
        {
            
            using var writer = new IndexWriter(Env, _analyzers);
            writer.UpdateDynamicFieldsMapping(IndexFieldsMappingBuilder.CreateForWriter(true)
                .AddDynamicBinding(dynamicSlice,FieldIndexingMode.No, true)
                .Build());
            foreach (var (id, item) in new[] {("1", blittable1), ("2", blittable2)})
            {
                using var builder = writer.Index(Encodings.Utf8.GetBytes(id));
                builder.Write(IndexId, null, Encodings.Utf8.GetBytes(id));
                builder.Store(Constants.IndexWriter.DynamicField, "Dynamic", item);
            }

            writer.Commit();
        }

        using var __ = Slice.From(bsc, "Dynamic", out var fieldName);
        
        Span<long> mem = stackalloc long[1024];
        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.SearchQuery(_analyzers.GetByFieldId(0).Metadata, new List<string>(){"1"}, Constants.Search.Operator.Or);
            Assert.Equal(1, match.Fill(mem));
            Page p = default;
            var result = searcher.GetEntryTermsReader(mem[0], ref p);
            long fieldRootPage = searcher.FieldCache.GetLookupRootPage(fieldName);
            Assert.True(result.FindNextStored(fieldRootPage));
            {
                var span = result.StoredField.Value;
                var reader = new BlittableJsonReaderObject(span.Address, span.Length, ctx);
                Assert.Equal(blittable1.Size, reader.Size);
                Assert.True(blittable1.Equals(reader));
            }
        }

        //Delete part
        using (var indexWriter = new IndexWriter(Env, _analyzers))
        {
            indexWriter.TryDeleteEntry("1");
            indexWriter.Commit();
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(1, match.Fill(mem));
            long id = mem[0];
            Assert.Equal("2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
        }
        
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void PhraseMatchPrimitiveTest()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        _analyzers = CreateKnownFields(_bsc, true, shouldStore: true);

        {
            using var writer = new IndexWriter(Env, _analyzers);

            using (var builder = writer.Index("us/1"u8))
            {
                builder.Write(IndexId, null, "us/1"u8);
                builder.Write(ContentId, null, "First second third fourth");
            }
            using (var builder = writer.Index("us/2"u8))
            {
                builder.Write(IndexId, null, "us/2"u8);
                builder.Write(ContentId, null, "First third fourth second");
            }
           
            writer.Commit();
        }

        {
            using var indexSearcher = new IndexSearcher(Env, _analyzers);
            var bytes = "secondthird"u8;
            var tokens = new global::Corax.Pipeline.Token[] {new Token() {Length = (uint)6, Offset = 0, Type = default}, new Token() {Length = 5, Offset = 6, Type = default}};

            var search = indexSearcher.PhraseQuery(indexSearcher.AllEntries(), _analyzers.GetByFieldId(ContentId).Metadata, bytes, tokens);
            Span<long> ids = stackalloc long[16];
            Assert.Equal(1, search.Fill(ids));
            Page page = default;
            var reader = indexSearcher.GetEntryTermsReader(ids[0], ref page);
        }
    }
    
    [Fact]
    public unsafe void CanStoreBlittableWithWriterScope()
    {
        using var ctx = JsonOperationContext.ShortTermSingleUse();
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);

        _analyzers = CreateKnownFields(_bsc, true, shouldStore: true);
        using var blittable1 = ctx.ReadObject(json1, "foo");
        using var blittable2 = ctx.ReadObject(json2, "foo");
        {
            
            using var writer = new IndexWriter(Env, _analyzers);

            foreach (var (id, item) in new[] {("1", blittable1), ("2", blittable2)})
            {
                using var builder = writer.Index(Encodings.Utf8.GetBytes(id));
                builder.Write(IndexId, null, Encodings.Utf8.GetBytes(id));
                builder.Store(item);
            }

            writer.Commit();
        }

        Span<long> mem = stackalloc long[1024];
        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.SearchQuery(_analyzers.GetByFieldId(0).Metadata, new List<string>(){"1"}, Constants.Search.Operator.Or);
            Assert.Equal(1, match.Fill(mem));
            Page p = default;
            var result = searcher.GetEntryTermsReader(mem[0], ref p);
            var fieldRootPage = searcher.FieldCache.GetLookupRootPage(_analyzers.GetByFieldId(1).FieldName);
            Assert.True(result.FindNextStored(fieldRootPage));

            {
                var span = result.StoredField.Value;
                var reader = new BlittableJsonReaderObject(span.Address, span.Length, ctx);
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
            indexWriter.TryDeleteEntry("1");
            indexWriter.Commit();
        }

        {
            using IndexSearcher searcher = new IndexSearcher(Env, _analyzers);
            var match = searcher.AllEntries();
            Assert.Equal(1, match.Fill(mem));
            long id = mem[0];
            Assert.Equal("2", searcher.TermsReaderFor(searcher.GetFirstIndexedFiledName()).GetTermFor(id));
        }
    }

    private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx, bool analyzers, bool shouldStore = false)
    {
        Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
        Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

        using var builder = (analyzers
            ? IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice, Analyzer.CreateDefaultAnalyzer(ctx))
                .AddBinding(ContentId, contentSlice, LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30)), shouldStore: shouldStore, fieldIndexingMode: FieldIndexingMode.Search)
            : IndexFieldsMappingBuilder.CreateForWriter(false)
                .AddBinding(IndexId, idSlice)
                .AddBinding(ContentId, contentSlice, fieldIndexingMode: FieldIndexingMode.No,shouldStore:shouldStore));
        
        return builder.Build();
    }

    public override void Dispose()
    {
        _bsc.Dispose();
        _analyzers.Dispose();
        base.Dispose();
    }
}
