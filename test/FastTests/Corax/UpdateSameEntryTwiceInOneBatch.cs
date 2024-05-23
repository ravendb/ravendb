using System;
using System.Collections.Generic;
using System.Linq;
using Corax;
using Corax.Querying;
using Corax.Mappings;
using FastTests.Voron;
using Sparrow.Server;
using Voron;
using Xunit.Abstractions;
using Xunit;
using Sparrow.Threading;
using Tests.Infrastructure;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

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
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void ManyUpdateToTheSameEntry()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                    builder.EndWriting();
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "fernando"u8);
                    builder.EndWriting();
                }
                writer.Commit();
            }
            
            Dictionary<long, string> fieldNamesByRootPage;
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "eagles"u8);
                    builder.EndWriting();
                }

                fieldNamesByRootPage = writer.GetIndexedFieldNamesByRootPage();

                writer.Commit();
            }
            
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "eagles"u8); // no change!
                    builder.EndWriting();
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

        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void CanWork()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                    builder.EndWriting();
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "fernando"u8);
                        builder.EndWriting();
                    }
                }

                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "eagles"u8);
                        builder.EndWriting();
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
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void CanRemoveDocumentUpdatedTwiceInSameBatch()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                    builder.EndWriting();
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "fernando"u8);
                        builder.EndWriting();
                    }
                }

                {
                    writer.TryDeleteEntryByField("Id", "users/1");
                }

                writer.Commit();
            }

            {
                Span<long> matches = stackalloc long[16];
                using var searcher = new IndexSearcher(Env, fields);
                Assert.Equal(0, searcher.NumberOfEntries);
                Span<long> ids = stackalloc long[16];
                var read = searcher.AllEntries().Fill(ids);
                Assert.Equal(0, read);

                read = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "fernando").Fill(ids);
                Assert.Equal(0, read);

                read = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "dancing queen").Fill(ids);
                Assert.Equal(0, read);
            }
        }
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void CanRemoveDocumentUpdatedTwiceInSameBatchNotByMainKey()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "dancing queen"u8);
                    builder.Write(2, "someKey"u8);
                    builder.EndWriting();
                }
                
                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                {
                    using (var builder = writer.Update("users/1"u8))
                    {
                        builder.Write(0, "users/1"u8);
                        builder.Write(1, "fernando"u8);
                        builder.Write(2, "someKey"u8);
                        builder.EndWriting();
                    }
                }

                {
                    writer.TryDeleteEntryByField("NumField", "someKey");
                }

                writer.Commit();
            }

            {
                Span<long> matches = stackalloc long[16];
                using var searcher = new IndexSearcher(Env, fields);
                Assert.Equal(0, searcher.NumberOfEntries);
                Span<long> ids = stackalloc long[16];
                var read = searcher.AllEntries().Fill(ids);
                Assert.Equal(0, read);

                read = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "fernando").Fill(ids);
                Assert.Equal(0, read);

                read = searcher.TermQuery(fields.GetByFieldId(1).Metadata, "dancing queen").Fill(ids);
                Assert.Equal(0, read);
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
        
        [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Indexes)]
        public void InsertNewDocumentIntoTermWhileUpdatingAnotherDocumentWithTheSameTerm()
        {
            var fields = CreateKnownFields(_bsc);
            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
                using (var builder = writer.Index("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "1"u8);
                    builder.EndWriting();
                }

                writer.Commit();
            }

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
            {
               
                using (var builder = writer.Update("users/1"u8))
                {
                    builder.Write(0, "users/1"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "2"u8);
                    builder.EndWriting();
                }
                using (var builder = writer.Update("users/2"u8))
                {
                    builder.Write(0, "users/2"u8);
                    builder.Write(1, "maciej"u8);
                    builder.Write(2, "0"u8);
                    builder.EndWriting();
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

            using (var writer = new IndexWriter(Env, fields, SupportedFeatures.All))
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
