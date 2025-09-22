using System;
using System.IO;
using Raven.Client.Documents;
using Raven.Server;
using Tests.Infrastructure.Operations;
using XunitLogger;

namespace Tests.Infrastructure.Utils
{
    public static class DebugPackageHandler
    {
        private const string DestinationDirectory = "debug_packages";

        public static void DownloadAndSave(RavenServer ravenServer, Context testContext)
        {
            using (var documentStore = InitDocumentStore(ravenServer))
            {
                var operationResult = documentStore.Maintenance.Server.Send(new GetClusterDebugInfoPackageOperation());

                SaveDebugPackage(testContext, operationResult);
            }
        }

        private static IDocumentStore InitDocumentStore(RavenServer ravenServer)
            => new DocumentStore { Urls = new[] { ravenServer.WebUrl }, Certificate = ravenServer.Certificate?.Certificate }
                .Initialize();

        private static void SaveDebugPackage(Context testContext, ClusterDebugInfoPackageResult operationResult)
        {
            if (Directory.Exists(DestinationDirectory) == false)
                Directory.CreateDirectory(DestinationDirectory);

            var fileName = GetDebugPackageName(testContext);
            var outputPath = Path.Join(DestinationDirectory, fileName);

            using (var fileStream = File.Create(outputPath))
            {
                operationResult.Stream.CopyTo(fileStream);
            }

            LogDebugPackageSaved(testContext, outputPath);
        }

        private static string GetDebugPackageName(Context testContext)
        {
            return $"{testContext.MethodName}_{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}.zip";
        }

        private static void LogDebugPackageSaved(Context testContext, string outputPath)
        {
            var fullPath = Path.GetFullPath(outputPath);
            var message = $"Saved debug package for {testContext.Test.DisplayName} in {fullPath}";

            testContext.TestOutput.WriteLine(message);
        }
    }
}
