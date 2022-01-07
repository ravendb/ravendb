using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Pipeline;
using Corax.Queries;
using FastTests.Voron;
using Microsoft.Diagnostics.Tracing.Parsers;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;
using Xunit;
using Xunit.Abstractions;
using WhitespaceTokenizer = Corax.Pipeline.WhitespaceTokenizer;

namespace FastTests.Corax
{
    public class MoreLikeThisTests : StorageTest
    {



        public MoreLikeThisTests(ITestOutputHelper output) : base(output)
        {
        }

        private IndexFieldsMapping CreateKnownAnalyzedFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Bio", ByteStringType.Immutable, out Slice bioSlice);

            return new IndexFieldsMapping(ctx)
                        .AddBinding(2, bioSlice, Analyzer.Create(default(WhitespaceTokenizer), default(LowerCaseTransformer)));
        }


        [Fact]
        public unsafe void CanQuerySimilarEntries()
        {
            using var ctx = JsonOperationContext.ShortTermSingleUse();

            using var allocator = new ByteStringContext(SharedMultipleUseFlag.None);
            var analyzers = CreateKnownAnalyzedFields(allocator); 

            using var indexWriter = new IndexWriter(Env, analyzers);
            PrepareData(indexWriter);
            using var indexSearcher = new IndexSearcher(Env);
            Span<long> match = stackalloc long[1024];

            // match on "shepherd" to arava
            var mlt = MoreLikeThisQuery.Build(indexSearcher, ctx.ReadObject(new DynamicJsonValue
            {
                ["Bio"] = "Reading shepherd novels",
            }, "test"), analyzers.GetByFieldId(2).Analyzer);
            Assert.Equal(1,mlt.Fill(match));
            Assert.Equal("arava", indexSearcher.GetIdentityFor(match[0]));
            
            // match on "running" to phoebe, zoof
            mlt = MoreLikeThisQuery.Build(indexSearcher, ctx.ReadObject(new DynamicJsonValue
            {
                ["Bio"] = "Running on the beach",
            }, "test"), analyzers.GetByFieldId(2).Analyzer);
            Assert.Equal(2,mlt.Fill(match));
            Assert.Equal("phoebe", indexSearcher.GetIdentityFor(match[0]));
            Assert.Equal("zoof", indexSearcher.GetIdentityFor(match[1]));

        }

        private void PrepareData(IndexWriter writer)
        {
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var knownFields = CreateKnownFields(bsc);
            const int bufferSize = 4096;
            using var _ = bsc.Allocate(bufferSize, out ByteString buffer);
            writer.Index("arava", CreateEntry(buffer, knownFields, "arava", "Arava Eini", "German shepherd & Barking", "Balls and walks"), knownFields);
            writer.Index("phoebe", CreateEntry(buffer, knownFields , "phoebe", "Phoebe Eini", "Barking and resting and running", "Moving as little as possible, balls"), knownFields);
            writer.Index("zoof", CreateEntry(buffer, knownFields, "zoof", "Zoof Orphan", "Running and licking and jumping", "Jumping on people"), knownFields);
            writer.Index("sunny", CreateEntry(buffer, knownFields, "sunny", "Sunny Inlaw", "Shedding and resting and eating", "Hiding under a chair"), knownFields);
            writer.Commit();
        }
        
        Span<byte> CreateEntry(ByteString buffer, IndexFieldsMapping knownFields, string id, string name, string bio, string interests)
        {
            var entryWriter = new IndexEntryWriter(buffer.ToSpan(), knownFields);
            entryWriter.Write(0, Encoding.UTF8.GetBytes(id));
            entryWriter.Write(1, Encoding.UTF8.GetBytes(name));
            entryWriter.Write(2, Encoding.UTF8.GetBytes(bio));
            entryWriter.Write(3, Encoding.UTF8.GetBytes(interests));
            entryWriter.Finish(out var output);
            return output;
        }


        private static string[] fields = new[] { "Id", "Name", "Bio", "Interests" }; 
        
        private IndexFieldsMapping CreateKnownFields(ByteStringContext bsc)
        {
            var analyzer = Analyzer.Create(default(WhitespaceTokenizer), default(LowerCaseTransformer));
            var mapping = new IndexFieldsMapping(bsc);
            for (int i = 0; i < fields.Length; i++)
            {
                Slice.From(bsc, fields[i], ByteStringType.Immutable, out Slice slice);
                mapping.AddBinding(i, slice, analyzer);
            }

            return mapping;
        }
    }
}
