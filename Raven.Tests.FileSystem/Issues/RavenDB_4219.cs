// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4219.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.FileSystem;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4219 : RavenFilesTestBase
    {
        private string[] sampleFiles = {"d.file", "c.file", "a.file", "b.file"};

        [Fact]
        public async Task order_by_fullPath()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                // ascending

                var results = await session.Query()
                                         .OrderBy(x => x.FullPath)
                                         .ToListAsync();

                assert_query_results(sampleFiles.OrderBy(x => x), results);

                // descending

                results = await session.Query()
                                             .OrderByDescending(x => x.FullPath)
                                             .ToListAsync();

                assert_query_results(sampleFiles.OrderByDescending(x => x), results);
            }
        }

        [Fact]
        public async Task order_by_creationDate()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                // ascending

                var results = await session.Query()
                                         .OrderBy(x => x.CreationDate)
                                         .ToListAsync();

                assert_query_results(sampleFiles, results);

                // descending

                results = await session.Query()
                                             .OrderByDescending(x => x.CreationDate)
                                             .ToListAsync();

                assert_query_results(sampleFiles.Reverse(), results);
            }
        }

        [Fact]
        public async Task order_by_directory()
        {
            var store = NewStore();

            string[] filesInsideDirectories = {"/items/b.file", "/a.file", "/items/duplicates/c.file", "/records/d.file"};

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, filesInsideDirectories);

                // ascending

                var results = await session.Query()
                                         .OrderBy(x => x.Directory)
                                         .ToListAsync();

                assert_query_results(new [] {"a.file", "b.file", "c.file", "d.file"}, results);

                // descending

                results = await session.Query()
                                             .OrderByDescending(x => x.Directory)
                                             .ToListAsync();

                assert_query_results(new[] { "d.file", "c.file", "b.file", "a.file" }, results);
            }
        }

        [Fact]
        public async Task order_by_etag()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                // ascending

                var results = await session.Query()
                                         .OrderBy(x => x.Etag)
                                         .ToListAsync();

                assert_query_results(sampleFiles, results);

                // descending

                results = await session.Query()
                                             .OrderByDescending(x => x.Etag)
                                             .ToListAsync();

                assert_query_results(sampleFiles.Reverse(), results);
            }
        }

        [Fact]
        public async Task does_not_allow_to_order_by_extension()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderBy(x => x.Extension).ToListAsync()));

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderByDescending(x => x.Extension).ToListAsync()));
            }
        }

        [Fact]
        public async Task does_not_allow_to_order_by_humaneTotalSize()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderBy(x => x.HumaneTotalSize).ToListAsync()));

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderByDescending(x => x.HumaneTotalSize).ToListAsync()));
            }
        }

        [Fact]
        public async Task does_not_allow_to_order_by_originalMetadata()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderBy(x => x.OriginalMetadata).ToListAsync()));

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderByDescending(x => x.OriginalMetadata).ToListAsync()));
            }
        }

        [Fact]
        public async Task does_not_allow_to_order_by_isTombstone()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderBy(x => x.IsTombstone).ToListAsync()));

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderByDescending(x => x.IsTombstone).ToListAsync()));
            }
        }

        [Fact]
        public async Task does_not_allow_to_order_by_uploadedSize()
        {
            var store = NewStore();

            using (var session = store.OpenAsyncSession())
            {
                await put_files(session, sampleFiles);

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderBy(x => x.UploadedSize).ToListAsync()));

                Assert.Throws<NotSupportedException>(() => AsyncHelpers.RunSync(() => session.Query().OrderByDescending(x => x.IsTombstone).ToListAsync()));
            }
        }

        private Task put_files(IAsyncFilesSession session, string[] files)
        {
            foreach (var file in files)
            {
                session.RegisterUpload(file, CreateRandomFileStream(3));
            }
            
            return session.SaveChangesAsync();
        }

        private void assert_query_results(IEnumerable<string> expected, List<FileHeader> actual)
        {
            Assert.Equal(expected.Count(), actual.Count);

            var i = 0;

            foreach (var file in expected)
            {
                Assert.Equal(file, actual[i++].Name);
            }
        }
    }
}