using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using FastTests.Voron;
using Raven.Client.Documents.Linq;
using Sparrow;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;

namespace FastTests.Corax
{
    public class UpdateSameEntryTwiceInOneBatch : StorageTest
    {
        private const int IndexId = 0, ContentId = 1;
        private readonly IndexFieldsMapping _analyzers;
        private readonly ByteStringContext _bsc;

        public UpdateSameEntryTwiceInOneBatch(ITestOutputHelper output) : base(output)
        {
            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _analyzers = CreateKnownFields(_bsc);
        }

        [Fact]
        public void CanWork()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields))
            {
                using var builder = new IndexEntryWriter(_bsc, fields);
                builder.Write(0, "users/1"u8);
                builder.Write(1, "dancing queen"u8);
                using var _ = builder.Finish(out var o);

                writer.Index("users/1"u8, o.ToSpan());

                writer.PrepareAndCommit();
            }

            using (var writer = new IndexWriter(Env, fields))
            {
                {
                    using var builder = new IndexEntryWriter(_bsc, fields);
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "fernando"u8);
                    using var _ = builder.Finish(out var o);

                    
                    writer.Update("users/1"u8, o.ToSpan());
                }

                {
                    using var builder = new IndexEntryWriter(_bsc, fields);
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "eagles"u8);
                    using var _ = builder.Finish(out var o);
                    writer.Update("users/1"u8, o.ToSpan());
                }

                writer.PrepareAndCommit();
            }

            {
                Span<long> matches = stackalloc long[16];
                using var searcher = new IndexSearcher(Env, fields);
                var eagles = searcher.TermQuery("Content", "eagles");
                Assert.Equal(1, eagles.Fill(matches));

                var fernando = searcher.TermQuery("Content", "fernando");
                Assert.Equal(0, fernando.Fill(matches));
            }
        }

        private static IndexFieldsMapping CreateKnownFields(ByteStringContext ctx)
        {
            Slice.From(ctx, "Id", ByteStringType.Immutable, out Slice idSlice);
            Slice.From(ctx, "Content", ByteStringType.Immutable, out Slice contentSlice);

            using (var builder = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(IndexId, idSlice).AddBinding(ContentId, contentSlice))
                return builder.Build();
        }

        public override void Dispose()
        {
            _bsc.Dispose();
            _analyzers.Dispose();
            base.Dispose();
        }
    }
}
