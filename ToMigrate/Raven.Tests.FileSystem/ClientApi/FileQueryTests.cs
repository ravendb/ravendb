using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.ClientApi
{
    public class FileQueryTests : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task CanQueryByName()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals(x => x.Name, "test.fil")
                                       .ToListAsync();

                Assert.Equal(1, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.fil", query.First().Name);
            }
        }

        [Fact]
        public async Task CanQueryByStartingWith()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereStartsWith(x => x.Name, "test.fil")
                                       .ToListAsync();

                Assert.Equal(2, stats.TotalResults);
                Assert.Equal(2, query.Count);

                query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereStartsWith(x => x.Name, "test.")
                                       .ToListAsync();

                Assert.Equal(4, stats.TotalResults);
                Assert.Equal(4, query.Count);

                query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereStartsWith(x => x.Name, "test.fi")
                                       .ToListAsync();

                Assert.Equal(3, stats.TotalResults);
                Assert.Equal(3, query.Count);
            }
        }

        [Theory]
        [InlineData(150)]
        [InlineData(1500)]
        [InlineData(2000)]
        [InlineData(3000)]
        public async Task CanGetFullStats(int total)
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.MaxNumberOfRequestsPerSession = total;
                for (var i = 0; i < total; i++)
                    session.RegisterUpload("test.file" + i, CreateUniformFileStream(1));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                FilesQueryStatistics stats;
                var query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereStartsWith(x => x.Name, "test.file")
                                       .ToListAsync();

                Assert.Equal(total, stats.TotalResults);
                Assert.Equal(Math.Min(total, 1024), query.Count);
            }
        }

        [Fact]
        public async Task CanQueryAll()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                       .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(4, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryAllOnDirectory()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("a/test.file", CreateUniformFileStream(10));
                session.RegisterUpload("b/test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("b/test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("b/test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OnDirectory("/b/")
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(3, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryRootDirectoryWithoutRecursive()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("b/test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("b/test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OnDirectory()
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryByMultipleWithOrStatement()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                           .WhereEquals(x => x.Name, "test.fil")
                                           .OrElse()
                                           .WhereEquals(x => x.Name, "test.file")
                                           .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereIn()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "test.file" })
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexOrClause()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "test.file" })
                                         .OrElse()
                                         .WhereEquals(x => x.Name, "test.fi")
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(3, query.Count());
                Assert.Contains("test.fi", query.Select(x => x.Name));
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexOrClauseInverted()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereEquals(x => x.Name, "test.fi")
                                         .OrElse()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "test.file" })
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(3, query.Count());
                Assert.Contains("test.fi", query.Select(x => x.Name));
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexAndClause()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "test.file" })
                                         .AndAlso()
                                         .WhereEquals(x => x.Name, "test.fil")
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexAndClauseInverted()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereEquals(x => x.Name, "test.fil")
                                         .AndAlso()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "test.file" })
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexStartWithClause()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("file.test", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereIn(x => x.Name, new[] { "test.fil", "file.test" })
                                         .AndAlso()
                                         .WhereStartsWith(x => x.Name, "test")
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryWhereInWithComplexStartWithClauseInverted()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("file.test", CreateUniformFileStream(10));
                session.RegisterUpload("file.test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereStartsWith(x => x.Name, "test")
                                         .OrElse()
                                         .WhereIn(x => x.Name, new[] { "file.test.fil", "file.test" })
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(4, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryInsideDirectory()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/test.file1", CreateUniformFileStream(10));
                session.RegisterUpload("dir/another/test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/another/test.file1", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                   .OnDirectory("dir", recursive: true)
                                   .WhereEquals(x => x.Name, "test.file")
                                   .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryStoppingAtDirectory()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/test.file1", CreateUniformFileStream(10));
                session.RegisterUpload("dir/another/test.file", CreateUniformFileStream(10));
                session.RegisterUpload("dir/another/test.file1", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                   .OnDirectory("dir")
                                   .WhereEquals(x => x.Name, "test.file")
                                   .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
            }
        }

        [Fact]
        public async Task CanQueryByExtension()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();


                Assert.Throws<NotSupportedException>(() => session.Query().WhereEquals(x => x.Extension, "fil"));

                var query = await session.Query().WhereEndsWith(x => x.Name, ".fil").ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.fil", query.First().Name);
            }
        }

        [Fact]
        public async Task CanQueryByMetadata()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                await session.SaveChangesAsync();

                var file1 = await session.LoadFileAsync("test.file");
                file1.Metadata["Test"] = true;

                var file2 = await session.LoadFileAsync("test.fil");
                file2.Metadata["Test"] = false;

                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .WhereEquals("Test", true)
                                         .ToListAsync();


                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.file", query.Select(x => x.Name));

                query = await session.Query()
                                         .WhereEquals("Test", false)
                                         .ToListAsync();

                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanQueryBySize()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                        .WhereGreaterThan(x => x.TotalSize, 150)
                                        .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.file", query.Select(x => x.Name));
                Assert.Contains("test.f", query.Select(x => x.Name));


                query = await session.Query()
                                       .WhereEquals(x => x.TotalSize, 150)
                                       .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));

                query = await session.Query()
                                       .WhereLessThan(x => x.TotalSize, 150)
                                       .ToListAsync();

                Assert.Equal(1, query.Count());
                Assert.Contains("test.fi", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async Task CanUseFirst()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var value = await session.Query()
                                         .Statistics(out stats)
                                         .WhereGreaterThan(x => x.TotalSize, 150)
                                         .FirstAsync();
                Assert.Equal(2, stats.TotalResults);

                var query = await session.Query()
                                         .WhereGreaterThan(x => x.TotalSize, 150)
                                         .ToListAsync();


                Assert.True(query.Any());
                Assert.Equal(value.Name, query[0].Name);

                await AssertAsync.Throws<InvalidOperationException>(async () =>
                    await session.Query()
                        .Statistics(out stats)
                        .WhereGreaterThan(x => x.TotalSize, 800)
                        .FirstAsync());
                Assert.Equal(0, stats.TotalResults);
            }
        }

        [Fact]
        public async Task CanUseFirstOrDefault()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var fileHeader = session.Query()
                    .Statistics(out stats)
                    .WhereGreaterThan(x => x.TotalSize, 10)
                    .FirstOrDefaultAsync()
                    .Result;
                Assert.Equal(4, stats.TotalResults);
                Assert.NotNull(fileHeader);

                Assert.Null(session.Query()
                    .Statistics(out stats)
                    .WhereGreaterThan(x => x.TotalSize, 700)
                    .FirstOrDefaultAsync()
                    .Result);
                Assert.Equal(0, stats.TotalResults);
            }
        }

        [Fact]
        public async Task CanUseSingle()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var fileHeader = session.Query()
                    .Statistics(out stats)
                    .WhereGreaterThan(x => x.TotalSize, 550)
                    .SingleAsync()
                    .Result;

                Assert.NotNull(fileHeader);
                Assert.Equal(1, stats.TotalResults);
                await AssertAsync.Throws<InvalidOperationException>(async () =>
                    await session.Query()
                        .Statistics(out stats)
                        .WhereGreaterThan(x => x.TotalSize, 150)
                        .SingleAsync());
                Assert.Equal(2, stats.TotalResults);

                await AssertAsync.Throws<InvalidOperationException>(async () =>
                    await session.Query()
                        .Statistics(out stats)
                        .WhereGreaterThan(x => x.TotalSize, 700)
                        .SingleAsync());
                Assert.Equal(0, stats.TotalResults);
            }
        }

        [Fact]
        public async Task CanUseSingleOrDefault()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var fileHeader = session.Query()
                    .Statistics(out stats)
                    .WhereGreaterThan(x => x.TotalSize, 550)
                    .SingleOrDefaultAsync()
                    .Result;

                Assert.NotNull(fileHeader);
                Assert.Equal(1, stats.TotalResults);

                Assert.Null(
                    session.Query()
                        .Statistics(out stats)
                        .WhereGreaterThan(x => x.TotalSize, 700)
                        .SingleOrDefaultAsync()
                        .Result);
                Assert.Equal(0, stats.TotalResults);

                await AssertAsync.Throws<InvalidOperationException>(async () =>
                    await session.Query()
                        .Statistics(out stats)
                        .WhereGreaterThan(x => x.TotalSize, 150)
                        .SingleOrDefaultAsync());
                Assert.Equal(2, stats.TotalResults);
            }
        }

        [Fact]
        public async Task CanUseOrderBySize()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderBy(x => x.TotalSize)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderBy(x => x.TotalSize);


                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseOrderByDescendingSize()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderByDescending(x => x.TotalSize)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderByDescending(x => x.TotalSize);


                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }


        [Fact]
        public async Task CanUseOrderByName()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("d.file", CreateUniformFileStream(100));
                session.RegisterUpload("c.file", CreateUniformFileStream(101));
                session.RegisterUpload("a.file", CreateUniformFileStream(102));
                session.RegisterUpload("b.file", CreateUniformFileStream(103));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderBy(x => x.Name)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderBy(x => x.Name);


                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseOrderByDescendingName()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("a.file", CreateUniformFileStream(100));
                session.RegisterUpload("b.file", CreateUniformFileStream(101));
                session.RegisterUpload("c.file", CreateUniformFileStream(102));
                session.RegisterUpload("d.file", CreateUniformFileStream(103));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderByDescending(x => x.Name)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderByDescending(x => x.Name);


                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseOrderByMultipleConditionsOnDescending()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("b.file", CreateUniformFileStream(100));
                session.RegisterUpload("a.file", CreateUniformFileStream(101));
                session.RegisterUpload("d.file", CreateUniformFileStream(101));
                session.RegisterUpload("c.file", CreateUniformFileStream(102));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderBy(x => x.TotalSize)
                                         .ThenByDescending(x => x.Name)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderBy(x => x.TotalSize)
                                              .ThenByDescending(x => x.Name);

                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseOrderByMultipleConditions()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("b.file", CreateUniformFileStream(100));
                session.RegisterUpload("a.file", CreateUniformFileStream(101));
                session.RegisterUpload("d.file", CreateUniformFileStream(101));
                session.RegisterUpload("c.file", CreateUniformFileStream(102));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderBy(x => x.TotalSize)
                                         .ThenBy(x => x.Name)
                                         .ToListAsync();

                var queryAll = await session.Query()
                                            .ToListAsync();
                var queryOrderAfter = queryAll.OrderBy(x => x.TotalSize)
                                              .ThenBy(x => x.Name);

                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseOrderByMultipleGroupConditions()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("a.file", CreateUniformFileStream(100));
                session.RegisterUpload("b.file", CreateUniformFileStream(101));
                session.RegisterUpload("c.file", CreateUniformFileStream(101));
                session.RegisterUpload("d.file", CreateUniformFileStream(102));
                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .OrderBy(x => x.TotalSize)
                                         .OrderByDescending(x => x.Name)
                                         .ToListAsync();

                var queryAll = await session.Query().ToListAsync();
                var queryOrderAfter = queryAll.OrderByDescending(x => x.Name);

                var inPairs = query.Zip(queryOrderAfter, (x, y) => new Tuple<FileHeader, FileHeader>(x, y));
                foreach (var pair in inPairs)
                {
                    Assert.Equal(pair.Item1.Name, pair.Item2.Name);
                    Assert.Equal(pair.Item1.TotalSize, pair.Item2.TotalSize);
                }
            }
        }

        [Fact]
        public async Task CanUseContainsAll()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("a.file", CreateUniformFileStream(100));
                session.RegisterUpload("b.file", CreateUniformFileStream(101));
                await session.SaveChangesAsync();

                var fileA = await session.LoadFileAsync("a.file");
                fileA.Metadata["Test"] = new RavenJArray("test1", "test2", "test3");

                var fileB = await session.LoadFileAsync("b.file");
                fileB.Metadata["Test"] = new RavenJArray("test1", "test3");

                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .ContainsAll("Test", new string[] { "test1", "test2" })
                                         .ToListAsync();

                Assert.Equal(1, query.Count);
            }
        }

        [Fact]
        public async Task CanUseContainsAny()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("a.file", CreateUniformFileStream(100));
                session.RegisterUpload("b.file", CreateUniformFileStream(101));
                session.RegisterUpload("c.file", CreateUniformFileStream(103));
                await session.SaveChangesAsync();

                var fileA = await session.LoadFileAsync("a.file");
                fileA.Metadata["Test"] = new RavenJArray(new string[] { "test3" });

                var fileB = await session.LoadFileAsync("b.file");
                fileB.Metadata["Test"] = new RavenJArray("test1", "test3");

                var fileC = await session.LoadFileAsync("c.file");
                fileC.Metadata["Test"] = new RavenJArray("test2", "test3");

                await session.SaveChangesAsync();

                var query = await session.Query()
                                         .ContainsAny("Test", new string[] { "test1", "test2" })
                                         .ToListAsync();

                Assert.Equal(2, query.Count);
            }
        }

        [Fact]
        public async Task CanUseTakeAndSkip()
        {
            var store = this.NewStore();

            for (int i = 0; i < 20; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload(i + ".file", CreateRandomFileStream(i));
                    session.RegisterUpload(i + ".txt", CreateRandomFileStream(i));
                    await session.SaveChangesAsync();
                }
            }

            int pageSize = 5;
            var files = new List<FileHeader>();

            using (var session = store.OpenAsyncSession())
            {
                for (int i = 0; i < 4; i++)
                {
                    var results = await session.Query().WhereEndsWith(x => x.Name, ".file").Skip(i * pageSize).Take(pageSize).ToListAsync();
                    files.AddRange(results);
                }

                Assert.Equal(20, files.Count);
                Assert.Equal(20, files.Select(x => x.Name).Distinct().Count());
            }
        }

        [Fact]
        public async Task CanSearchByNumericMetadataFields()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var metadata = new RavenJObject();

                    metadata.Add("Int", 5);
                    metadata.Add("Long", 5L);
                    metadata.Add("Float", 5.0f);
                    metadata.Add("Double", 5.0);

                    metadata.Add("Uint", 5u);
                    metadata.Add("Ulong", 5UL);
                    metadata.Add("Short", (short) 5);
                    metadata.Add("Ushort", (ushort) 5);
                    metadata.Add("Decimal", 5m);

                    session.RegisterUpload("test-1.file", CreateRandomFileStream(10), metadata);

                    var metadata2 = new RavenJObject();

                    metadata2.Add("Int", 10);
                    metadata2.Add("Long", 10L);
                    metadata2.Add("Float", 10.0f);
                    metadata2.Add("Double", 10.0);

                    metadata2.Add("Uint", 10u);
                    metadata2.Add("Ulong", 10UL);
                    metadata2.Add("Short", (short) 10);
                    metadata2.Add("Ushort", (ushort) 10);
                    metadata2.Add("Decimal", 10m);

                    session.RegisterUpload("test-2.file", CreateRandomFileStream(10), metadata2);

                    await session.SaveChangesAsync();
                }

                var metadataKeys = new[]
                {
                    "Int", "Long", "Float", "Double", "Uint", "Ulong", "Short", "Ushort", "Decimal"
                };

                foreach (var key in metadataKeys)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        Assert.Equal(1, (await session.Query().WhereEquals(key, 5).ToListAsync()).Count);
                        Assert.Equal(1, (await session.Query().WhereGreaterThan(key, 5).ToListAsync()).Count);
                        Assert.Equal(2, (await session.Query().WhereGreaterThanOrEqual(key, 5).ToListAsync()).Count);
                        Assert.Equal(1, (await session.Query().WhereLessThan(key, 10).ToListAsync()).Count);
                        Assert.Equal(2, (await session.Query().WhereLessThanOrEqual(key, 10).ToListAsync()).Count);
                        Assert.Equal(0, (await session.Query().WhereBetween(key, 5, 10).ToListAsync()).Count);
                        Assert.Equal(1, (await session.Query().WhereBetween(key, 0, 10).ToListAsync()).Count);
                        Assert.Equal(1, (await session.Query().WhereBetween(key, 5, 20).ToListAsync()).Count);
                        Assert.Equal(2, (await session.Query().WhereBetweenOrEqual(key, 5, 10).ToListAsync()).Count);
                    }
                }
            }
        }

        [Fact]
        public async Task CanSearchInMetadataArrayFields1()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                var metadata = new RavenJObject
                {
                    {"Test", new RavenJArray { 1, 2 } }
                };
                session.RegisterUpload("test.file", CreateUniformFileStream(10), metadata);
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals("Test", 1)
                                       .ToListAsync();

                Assert.Equal(1, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.file", query.First().Name);

                query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals("Test", 2)
                                       .ToListAsync();

                Assert.Equal(1, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.file", query.First().Name);
            }
        }

        [Fact]
        public async Task CanSearchInMetadataArrayFields2()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                var metadata1 = new RavenJObject
                {
                    {"Test", new RavenJArray { 1, 2 } }
                };
                session.RegisterUpload("test1.file", CreateUniformFileStream(10), metadata1);

                var metadata2 = new RavenJObject
                {
                    {"Test", new RavenJArray { 1, 3 } }
                };
                session.RegisterUpload("test2.file", CreateUniformFileStream(10), metadata2);
                await session.SaveChangesAsync();

                FilesQueryStatistics stats;
                var query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals("Test", 1)
                                       .ToListAsync();

                Assert.Equal(2, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Equal("test1.file", query[0].Name);
                Assert.Equal("test2.file", query[1].Name);

                query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals("Test", 2)
                                       .ToListAsync();

                Assert.Equal(1, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test1.file", query.First().Name);

                query = await session.Query()
                                       .Statistics(out stats)
                                       .WhereEquals("Test", 3)
                                       .ToListAsync();

                Assert.Equal(1, stats.TotalResults);
                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test2.file", query.First().Name);
            }
        }
    }
}