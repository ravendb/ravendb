using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Corax;
using Corax.Mappings;
using Corax.Queries;
using Corax.Utils;
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
        private const int IndexId = 0, ContentId = 1, NumId = 2;
        private readonly IndexFieldsMapping _analyzers;
        private readonly ByteStringContext _bsc;

        public UpdateSameEntryTwiceInOneBatch(ITestOutputHelper output) : base(output)
        {
            _bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            _analyzers = CreateKnownFields(_bsc);
        }
        
        [Fact]
        public void ManyUpdateToTheSameEntry()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "fernando"u8);
                }
                writer.Commit();
            }
            
            Dictionary<long, string> fieldNamesByRootPage;
            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "eagles"u8);
                }

                fieldNamesByRootPage = writer.GetIndexedFieldNamesByRootPage();

                writer.Commit();
            }
            
            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "eagles"u8); // no change!
                }
                writer.Commit();
            }


            {
                Span<long> matches = stackalloc long[16];
                using var searcher = new IndexSearcher(Env, fields);
                var eagles = searcher.TermQuery("Content", "eagles");
                Assert.Equal(1, eagles.Fill(matches));
                Page p = default;
                var reader = searcher.GetEntryTermsReader(matches[0], ref p);
                long contentFieldRootPage = fieldNamesByRootPage.Single(x=>x.Value == "Content").Key;
                Assert.True(reader.FindNext(contentFieldRootPage));
                Assert.Equal("eagles", reader.Current.ToString());

                var fernando = searcher.TermQuery("Content", "fernando");
                Assert.Equal(0, fernando.Fill(matches));
                
            }
        }

        [Fact]
        public void CanWork()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields))
            {
                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "fernando"u8);
                    }
                }

                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "eagles"u8);
                    }
                }

                writer.Commit();
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
            Slice.From(ctx, "NumField", ByteStringType.Immutable, out Slice numFieldSlice);

            using (var builder = IndexFieldsMappingBuilder.CreateForWriter(false).AddBinding(IndexId, idSlice).AddBinding(ContentId, contentSlice).AddBinding(NumId, numFieldSlice))
                return builder.Build();
        }
        
        [Fact]
        public void InsertNewDocumentIntoTermWhileUpdatingAnotherDocumentWithTheSameTerm()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "1"u8);

                }

                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields))
            {
               
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "2"u8);
                }
                using (var builder = writer.Update("users/2"u8))
                {
                    builder.Write(0, "users/2"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "0"u8);
                }

                writer.Commit();
            }

            using (var searcher = new IndexSearcher(Env, fields))
            {
                var match = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "maciej");
                Span<long> ids = stackalloc long[4];
                var read = match.Fill(ids);
                Assert.Equal(2, read);

                Page p = default;

                var reader = searcher.GetEntryTermsReader(ids[0], ref p);
                var output = reader.Debug(searcher);
                Assert.Contains("Content", output);
                Assert.Contains("maciej", output);
            }

            using (var writer = new IndexWriter(Env, fields))
            {
                Assert.True(writer.TryDeleteEntry("users/1"u8));
                Assert.True(writer.TryDeleteEntry("users/2"u8));
                writer.Commit();
            }

            using (var searcher = new IndexSearcher(Env, fields))
            {
                var match = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "maciej");
                Span<long> ids = stackalloc long[4];
                var read = match.Fill(ids);
                Assert.Equal(0, read);
            }

        }

        public override void Dispose()
        {
            _bsc.Dispose();
            _analyzers.Dispose();
            base.Dispose();
        }
    }
}
