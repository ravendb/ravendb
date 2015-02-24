using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

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

                var query = await session.Query()
                                       .WhereEquals(x => x.Name, "test.fil")
                                       .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.fil", query.First().Name);
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
                                           .WhereEquals (x => x.Name, "test.fil")
                                           .OrElse()
                                           .WhereEquals (x => x.Name, "test.file")
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
                                         .WhereIn(x => x.Name, new [] { "test.fil", "test.file"})
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
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

                var value = await session.Query()
                                         .WhereGreaterThan(x => x.TotalSize, 150)
                                         .FirstAsync();

                var query = await session.Query()
                                         .WhereGreaterThan(x => x.TotalSize, 150)
                                         .ToListAsync();

                Assert.True(query.Any());
                Assert.Equal(value.Name, query[0].Name);
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

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 10).FirstOrDefaultAsync().Result);
                Assert.Null(session.Query().WhereGreaterThan(x => x.TotalSize, 700).FirstOrDefaultAsync().Result);
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

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 550).SingleAsync().Result);
                await AssertAsync.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 150).SingleAsync());
                await AssertAsync.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 700).SingleAsync());
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

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 550).SingleOrDefaultAsync().Result);
                Assert.Null(session.Query().WhereGreaterThan(x => x.TotalSize, 700).SingleOrDefaultAsync().Result);
                await AssertAsync.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 150).SingleOrDefaultAsync());
                
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

    }
}
