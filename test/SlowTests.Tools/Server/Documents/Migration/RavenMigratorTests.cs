using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tools.Server.Documents.Migration
{
    public class RavenMigratorTests : RavenTestBase
    {
        public RavenMigratorTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public async Task Migrate_ThroughServer_ShouldValidateConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                // valid path
                await RunMigratorThroughServer(store, new DynamicJsonValue { ["Command"] = "validateMigratorPath" });

                var e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer(store, new DynamicJsonValue { ["Command"] = "validateMigratorPath" }, migratorFullPath: Path.Combine(NewDataPath(), "missing")));
                Assert.Contains("doesn't exist", e.Message);

                e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer(store, new DynamicJsonValue { ["Command"] = "unknown", ["ConsoleExport"] = true }));
                Assert.Contains("Command 'unknown' doesn't exist", e.Message);

                e = await Assert.ThrowsAnyAsync<Exception>(() =>
                    RunMigratorThroughServer(store, new DynamicJsonValue { ["Command"] = "collections", ["ConsoleExport"] = true }));
                Assert.Contains("DatabaseName cannot be null or empty", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.BackupExportImport)]
        public void Migrator_WhenExportingToConsoleAndFile_ShouldFail()
        {
            var inputConfiguration = Newtonsoft.Json.JsonConvert.SerializeObject(new
            {
                Command = "export",
                AzureEndpointUrl = "https://localhost:1/",
                PrimaryKey = "key",
                DatabaseName = "db",
                ConsoleExport = true,
                ExportFilePath = NewDataPath()
            });

            // the endpoint is unreachable as well, so the exit code alone doesn't prove the validation
            var originalError = Console.Error;
            var error = new StringWriter();
            Console.SetError(error);
            try
            {
                Assert.Equal(-1, Raven.Migrator.Program.Main(new[] { "cosmosdb", "-j", inputConfiguration }));
            }
            finally
            {
                Console.SetError(originalError);
            }

            Assert.Contains("Cannot export to the console and to a file", error.ToString());
        }

        /// <summary>
        /// The endpoint used by the Studio: the server runs Raven.Migrator and passes the configuration through the standard input
        /// </summary>
        private static async Task RunMigratorThroughServer(IDocumentStore store, DynamicJsonValue inputConfiguration, string migratorFullPath = null)
        {
            var requestExecutor = store.GetRequestExecutor();
            using (requestExecutor.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var configuration = context.ReadObject(new DynamicJsonValue
                {
                    ["DatabaseTypeName"] = "cosmosdb",
                    // Raven.Migrator is copied to the tests output directory
                    ["MigratorFullPath"] = migratorFullPath ?? AppContext.BaseDirectory,
                    ["InputConfiguration"] = inputConfiguration
                }, "migration-configuration");

                await requestExecutor.ExecuteAsync(new MigrateCommand(configuration), context);
            }
        }

        private class MigrateCommand : RavenCommand
        {
            private readonly BlittableJsonReaderObject _configuration;

            public MigrateCommand(BlittableJsonReaderObject configuration)
            {
                _configuration = configuration;
            }

            public override bool IsReadRequest => false;

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                url = $"{node.Url}/databases/{node.Database}/admin/smuggler/migrate";

                return new HttpRequestMessage
                {
                    Method = HttpMethod.Post,
                    Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _configuration).ConfigureAwait(false), DocumentConventions.Default)
                };
            }
        }
    }
}
