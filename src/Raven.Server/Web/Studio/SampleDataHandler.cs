using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.Smuggler.Documents.Data;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Web.Studio
{
    public class SampleDataHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/studio/sample-data", "POST", AuthorizationStatus.ValidUser)]
        public async Task PostCreateSampleData()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    foreach (var collection in Database.DocumentsStorage.GetCollections(context))
                    {
                        if (collection.Count > 0)
                        {
                            throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");
                        }
                    }
                }
                
                var editRevisions = new EditRevisionsConfigurationCommand(new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Orders"] = new RevisionsCollectionConfiguration
                        {
                            Disabled = false
                        }
                    }
                }, Database.Name);
                await ServerStore.SendToLeaderAsync(editRevisions);

                using (var sampleData = typeof(SampleDataHandler).GetTypeInfo().Assembly
                    .GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.Northwind.ravendbdump"))
                {
                    using (var stream = new GZipStream(sampleData, CompressionMode.Decompress))
                    using (var source = new StreamSource(stream, context, Database))
                    {
                        var destination = new DatabaseDestination(Database);

                        var smuggler = new DatabaseSmuggler(Database, source, destination, Database.Time,
                            options: new DatabaseSmugglerOptionsServerSide
                            {
                                OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Attachments |
                                                 DatabaseItemType.Indexes,
                                SkipRevisionCreation = true
                            });

                        smuggler.Execute();
                    }
                }
                await NoContent();
            }
        }
        
        [RavenAction("/databases/*/studio/sample-data/classes", "GET", AuthorizationStatus.ValidUser)]
        public async Task GetSampleDataClasses()
        {
            using (var sampleData = typeof(SampleDataHandler).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.NorthwindModel.cs"))
            using (var responseStream = ResponseBodyStream())
            {
                HttpContext.Response.ContentType = "text/plain";
                await sampleData.CopyToAsync(responseStream);
            }
        }
    }
}
