using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.ClientApi
{
    public class FileQueryTests : RavenFsTestBase
    {
        [Fact]
        public async void CanQueryByName()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = session.Query()
                                   .WhereEquals(x => x.Name, "test.fil")
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.fil", query.First().Name);
            }
        }

        [Fact]
        public async void CanQueryByMultipleWithOrStatement()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(10));
                session.RegisterUpload("test.fil", CreateUniformFileStream(10));
                session.RegisterUpload("test.fi", CreateUniformFileStream(10));
                session.RegisterUpload("test.f", CreateUniformFileStream(10));
                await session.SaveChangesAsync();

                var query = session.Query()
                                   .WhereEquals (x => x.Name, "test.fil")
                                   .OrElse()
                                   .WhereEquals (x => x.Name, "test.file")
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));
                Assert.Contains("test.file", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async void CanQueryInsideDirectory()
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

                var query = session.Query()
                                   .OnDirectory("dir", recursive: true)
                                   .WhereEquals(x => x.Name, "test.file")
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
            }
        }

        [Fact]
        public async void CanQueryStoppingAtDirectory()
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

                var query = session.Query()
                                   .OnDirectory("dir")
                                   .WhereEquals(x => x.Name, "test.file")
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
            }
        }

        [Fact]
        public async void CanQueryByExtension()
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

                var query = session.Query().WhereEndsWith(x => x.Name, ".fil")
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Equal("test.fil", query.First().Name);
            }
        }

        [Fact]
        public async void CanQueryBySize()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                var query = session.Query()
                                   .WhereGreaterThan(x => x.TotalSize, 150)
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(2, query.Count());
                Assert.Contains("test.file", query.Select(x => x.Name));
                Assert.Contains("test.f", query.Select(x => x.Name));


                query = session.Query()
                               .WhereEquals(x => x.TotalSize, 150)
                               .ToList();

                Assert.True(query.Any());
                Assert.Equal(1, query.Count());
                Assert.Contains("test.fil", query.Select(x => x.Name));

                query = session.Query()
                               .WhereLessThan(x => x.TotalSize, 150)
                               .ToList();

                Assert.Equal(1, query.Count());
                Assert.Contains("test.fi", query.Select(x => x.Name));
            }
        }

        [Fact]
        public async void CanUseFirst()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                var value = session.Query()
                                   .WhereGreaterThan(x => x.TotalSize, 150)
                                   .First();

                var query = session.Query()
                                   .WhereGreaterThan(x => x.TotalSize, 150)
                                   .ToList();

                Assert.True(query.Any());
                Assert.Equal(value.Name, query[0].Name);
            }
        }

        [Fact]
        public async void CanUseFirstOrDefault()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 10).FirstOrDefault());
                Assert.Null(session.Query().WhereGreaterThan(x => x.TotalSize, 700).FirstOrDefault());
            }
        }

        [Fact]
        public async void CanUseSingle()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 550).Single());
                Assert.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 150).Single());
                Assert.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 700).Single());
            }
        }

        [Fact]
        public async void CanUseSingleOrDefault()
        {
            var store = this.NewStore();

            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload("test.file", CreateUniformFileStream(600));
                session.RegisterUpload("test.fil", CreateUniformFileStream(150));
                session.RegisterUpload("test.fi", CreateUniformFileStream(16));
                session.RegisterUpload("test.f", CreateUniformFileStream(330));
                await session.SaveChangesAsync();

                Assert.NotNull(session.Query().WhereGreaterThan(x => x.TotalSize, 550).SingleOrDefault());
                Assert.Null(session.Query().WhereGreaterThan(x => x.TotalSize, 700).SingleOrDefault());
                Assert.Throws<InvalidOperationException>(() => session.Query().WhereGreaterThan(x => x.TotalSize, 150).SingleOrDefault());
                
            }
        }
    }
}
