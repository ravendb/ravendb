using System;
using System.IO;
using Raven.Client.Documents;
using Raven.Server;
using Tests.Infrastructure.Operations;
using Xunit;

namespace Tests.Infrastructure.Utils
{
    public static class DebugPackageHandler
    {
        private const string DestinationDirectory = "debug_packages";

        public static void DownloadAndSave(RavenServer ravenServer, ITestContext testContext)
        {
            using (var documentStore = InitDocumentStore(ravenServer))
            {
                var operationResult = documentStore.Maintenance.Server.Send(new GetClusterDebugInfoPackageOperation());

                SaveDebugPackage(testContext, operationResult);
            }
        }

        private static IDocumentStore InitDocumentStore(RavenServer ravenServer)
            => new DocumentStore { Urls = new[] { ravenServer.WebUrl }, Certificate = ravenServer.Certificate?.ClientCertificate }
                .Initialize();

        private static void SaveDebugPackage(ITestContext testContext, ClusterDebugInfoPackageResult operationResult)
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

        private static string GetDebugPackageName(ITestContext testContext)
        {
            var methodName = (testContext?.TestMethod as Xunit.v3.IXunitTestMethod)?.Method?.Name ?? "UnknownMethod";
            return $"{methodName}_{DateTimeOffset.UtcNow.ToUnixTimeSeconds()}.zip";
        }

        private static void LogDebugPackageSaved(ITestContext testContext, string outputPath)
        {
            var fullPath = Path.GetFullPath(outputPath);
            var displayName = testContext?.Test?.TestDisplayName ?? "Unknown";
            var message = $"Saved debug package for {displayName} in {fullPath}";

            testContext?.TestOutputHelper?.WriteLine(message);
        }
    }
}
