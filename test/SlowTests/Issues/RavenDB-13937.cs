using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Util;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using Xunit;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13937 : RavenLowLevelTestBase
    {
        public RavenDB_13937(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task SearchForPageShouldNotSkipLastPage()
        {
            using (var inputStream = GetDump("RavenDB-13937.ravendbdump"))
            using (var stream = new GZipStream(inputStream, CompressionMode.Decompress))
            {
                using (var database = CreateDocumentDatabase())
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var source = new StreamSource(stream, context, database))
                {
                    var editRevisions = new EditRevisionsConfigurationCommand(new RevisionsConfiguration
                    {
                        Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                        {
                            ["Orders"] = new RevisionsCollectionConfiguration
                            {
                                Disabled = false
                            }
                        }
                    }, database.Name, RaftIdGenerator.NewId());

                    var (index, _) = await database.ServerStore.SendToLeaderAsync(editRevisions);
                    await database.RachisLogIndexNotifications.WaitForIndexNotification(index, database.ServerStore.Engine.OperationTimeout);

                    var destination = new DatabaseDestination(database);

                    var smuggler = await (new DatabaseSmuggler(database, source, destination, database.Time, new DatabaseSmugglerOptionsServerSide
                    {
                        OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Attachments |
                                         DatabaseItemType.Indexes,
                        SkipRevisionCreation = true
                    }).ExecuteAsync());

                    using (context.OpenReadTransaction())
                    {
                        var (revisions, count) = database.DocumentsStorage.RevisionsStorage.GetRevisions(context, "Orders/825-A", 0, int.MaxValue);

                        Assert.Equal(count, revisions.Length);
                    }
                }
            }
        }

        private static Stream GetDump(string name)
        {
            var assembly = typeof(RavenDB_13937).Assembly;
            return assembly.GetManifestResourceStream("SlowTests.Data." + name);
        }
    }
}
