// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2115.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.FileSystem;
using Raven.Json.Linq;
using Raven.Migration.MigrationTasks;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2115 : RavenTest
    {
        [Fact]
        public async Task CanMigrateAttachmentsToFileSystemOnSameServer()
        {
            using (var store = NewRemoteDocumentStore())
            using (var fsStore = new FilesStore()
            {
                Url = store.Url,
                DefaultFileSystem = "RavenDB_2115"
            }.Initialize(true))
            {
                var random = new Random();
                var bytes = new byte[137];

                random.NextBytes(bytes);

                for (int i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.PutAttachment("files/" + i, null, new MemoryStream(bytes), new RavenJObject());
                }

                new CopyAttachmentsToFileSystem(new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase
                }, new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                }, "RavenDB_2115", false, 1).Execute();

                Assert.Equal(10, store.DatabaseCommands.GetStatistics().CountOfAttachments);

                var browseResult = await fsStore.AsyncFilesCommands.BrowseAsync();

                Assert.Equal(10, browseResult.Length);

                for (int i = 10; i < 20; i++)
                {
                    store.DatabaseCommands.PutAttachment("files/" + i, null, new MemoryStream(bytes), new RavenJObject());
                }

                new CopyAttachmentsToFileSystem(new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase
                }, new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                }, "RavenDB_2115", true, 5).Execute();

                Assert.Equal(0, store.DatabaseCommands.GetStatistics().CountOfAttachments);

                browseResult = await fsStore.AsyncFilesCommands.BrowseAsync();

                Assert.Equal(20, browseResult.Length);
            }	
        }

        [Fact]
        public async Task CanMigrateAttachmentsToFileSystemOnDifferentServer()
        {
            using (var store = NewRemoteDocumentStore())
            using (GetNewServer(8078))
            using (var fsStore = new FilesStore()
            {
                Url = "http://localhost:8078",
                DefaultFileSystem = "RavenDB_2115"
            }.Initialize(true))
            {
                var random = new Random();
                var bytes = new byte[137];

                random.NextBytes(bytes);

                for (int i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.PutAttachment("files/" + i, null, new MemoryStream(bytes), new RavenJObject());
                }

                new CopyAttachmentsToFileSystem(new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase
                }, new RavenConnectionStringOptions()
                {
                    Url = "http://localhost:8078",
                }, "RavenDB_2115", false, 1).Execute();

                Assert.Equal(10, store.DatabaseCommands.GetStatistics().CountOfAttachments);

                var browseResult = await fsStore.AsyncFilesCommands.BrowseAsync();

                Assert.Equal(10, browseResult.Length);

                for (int i = 10; i < 20; i++)
                {
                    store.DatabaseCommands.PutAttachment("files/" + i, null, new MemoryStream(bytes), new RavenJObject());
                }

                new CopyAttachmentsToFileSystem(new RavenConnectionStringOptions()
                {
                    Url = store.Url,
                    DefaultDatabase = store.DefaultDatabase
                }, new RavenConnectionStringOptions()
                {
                    Url = "http://localhost:8078",
                }, "RavenDB_2115", true, 3).Execute();

                Assert.Equal(0, store.DatabaseCommands.GetStatistics().CountOfAttachments);

                browseResult = await fsStore.AsyncFilesCommands.BrowseAsync();

                Assert.Equal(20, browseResult.Length);
            }
        }
    }
}
