using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO.Compression;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents;
using Raven.Server.SqlMigration;
using Sparrow.Json;
using Voron.Exceptions;
using DatabaseSmuggler = Raven.Server.Smuggler.Documents.DatabaseSmuggler;

namespace Raven.Server.Web.Studio
{
    public class SampleDataHandler : DatabaseRequestHandler
    {

        [RavenAction("/databases/*/studio/sample-data", "POST", AuthorizationStatus.ValidUser)]
        public Task PostCreateSampleData()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                using (context.OpenReadTransaction())
                {
                    foreach (var collection in Database.DocumentsStorage.GetCollections(context))
                    {
                        if (collection.Count > 0 && collection.Name != CollectionName.SystemCollection)
                        {
                            throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");
                        }
                    }
                }

                using (var sampleData = typeof(SampleDataHandler).GetTypeInfo().Assembly.GetManifestResourceStream("Raven.Server.Web.Studio.EmbeddedData.Northwind_3.5.35168.ravendbdump"))
                {
                    using (var stream = new GZipStream(sampleData, CompressionMode.Decompress))
                    {
                        var source = new StreamSource(stream, context, Database);
                        var destination = new DatabaseDestination(Database);

                        var smuggler = new DatabaseSmuggler(source, destination, Database.Time);

                        smuggler.Execute();
                    }
                }

                return NoContent();
            }
        }

        [RavenAction("/databases/*/studio/sql-schema", "POST", AuthorizationStatus.DatabaseAdmin)]
        public Task SqlSchema()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                /*using (context.OpenReadTransaction())
                {
                    foreach (var collection in Database.DocumentsStorage.GetCollections(context))
                    {
                        if (collection.Count > 0 && collection.Name != CollectionName.SystemCollection)
                        {
                            throw new InvalidOperationException("You cannot create sample data in a database that already contains documents");
                        }
                    }
                }*/

                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);
                sqlImportDoc.TryGet("ConnectionString", out string connectionString);

                var connectionFactory = new ConnectionFactory(connectionString);

                var con = (SqlConnection) connectionFactory.OpenConnection();

                var db = new SqlDatabase(con);
                
                var tablesNames = new List<string>();
                var relations = new List<Tuple<string, string>>();

                foreach (var table in db.Tables)
                {
                    tablesNames.Add(table.Name);
                    if (table.References.Count > 0)
                        foreach (var reference in table.References)
                        {
                            if (table.Name != reference.Value.Item1)
                                relations.Add(Tuple.Create(table.Name, reference.Value.Item1));
                        }
                }

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName("Relations");

                    writer.WriteStartArray();
                    var first = true;
                    foreach (var item in relations)
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;
                        writer.WriteStartObject();
                        writer.WritePropertyName("Parent");
                        writer.WriteString(item.Item1);
                        writer.WriteComma();
                        writer.WritePropertyName("Child");
                        writer.WriteString(item.Item2);
                        writer.WriteEndObject();
                    }

                    writer.WriteEndArray();

                    writer.WriteComma();

                    writer.WritePropertyName("Tables");
                    writer.WriteStartArray();
                    first = true;
                    foreach (var item in tablesNames)
                    {
                        if (first == false)
                            writer.WriteComma();

                        first = false;
                        writer.WriteString(item);
                    }

                    writer.WriteEndArray();

                    writer.WriteComma();

                    writer.WritePropertyName("TableCount");
                    writer.WriteInteger(tablesNames.Count);

                    writer.WriteEndObject();
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/studio/import-sql", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ImportSql()
        {

            var sw = new Stopwatch();
            sw.Start();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var sqlImportDoc = context.ReadForDisk(RequestBodyStream(), null);
                sqlImportDoc.TryGet("ConnectionString", out string connectionString);
                sqlImportDoc.TryGet("Relations", out BlittableJsonReaderArray relations);
                if (!sqlImportDoc.TryGet("BinaryToAttachment", out bool binaryToAttachment))
                    binaryToAttachment = false;

                var connectionFactory = new ConnectionFactory(connectionString);

                var con = (SqlConnection) connectionFactory.OpenConnection();

                var db = new SqlDatabase(con);

                foreach (BlittableJsonReaderObject item in relations.Items)
                {
                    item.TryGet("Parent", out string parent);
                    item.TryGet("Child", out string child);
                    item.TryGet("PropertyName", out string propertyName);

                    try
                    {
                        db.Embed(parent, propertyName, child);
                    }
                    catch(Exception e) { }
                }

                //db.Embed("dbo.Orders", "Lines", "dbo.Order Details");
                //db.Embed("dbo.Products", "Lines", "dbo.Order Details");
                //db.Embed("dbo.Categories", "MyProducts", "dbo.Products");

                var databaseWriter = new RavenDatabaseWriter(db, Database, context, binaryToAttachment);

                await databaseWriter.WriteDatabase();
            }

            Console.WriteLine("Over all time: " + (double)sw.ElapsedMilliseconds / 1000);
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
