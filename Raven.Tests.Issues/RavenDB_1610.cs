// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1610.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_1610 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void AttachmentsCountInStatsShouldWorkProperly(string requestedStorage)
        {
            using (var store = NewDocumentStore(requestedStorage: requestedStorage))
            {
                var stats = store.DatabaseCommands.GetStatistics();
                Assert.NotNull(stats);
                Assert.Equal(0, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfAttachments);

                store.DatabaseCommands.Put("docs/1", null, new RavenJObject(), new RavenJObject());
                store.DatabaseCommands.PutAttachment("static/1", null, new MemoryStream(), new RavenJObject());
                store.DatabaseCommands.PutAttachment("static/2", null, new MemoryStream(), new RavenJObject());

                stats = store.DatabaseCommands.GetStatistics();
                Assert.NotNull(stats);
                Assert.Equal(1, stats.CountOfDocuments);
                Assert.Equal(2, stats.CountOfAttachments);

                store.DatabaseCommands.Delete("docs/1", null);

                stats = store.DatabaseCommands.GetStatistics();
                Assert.NotNull(stats);
                Assert.Equal(0, stats.CountOfDocuments);
                Assert.Equal(2, stats.CountOfAttachments);

                store.DatabaseCommands.DeleteAttachment("static/2", null);

                stats = store.DatabaseCommands.GetStatistics();
                Assert.NotNull(stats);
                Assert.Equal(0, stats.CountOfDocuments);
                Assert.Equal(1, stats.CountOfAttachments);

                store.DatabaseCommands.DeleteAttachment("static/1", null);

                stats = store.DatabaseCommands.GetStatistics();
                Assert.NotNull(stats);
                Assert.Equal(0, stats.CountOfDocuments);
                Assert.Equal(0, stats.CountOfAttachments);
            }
        }
    }
}