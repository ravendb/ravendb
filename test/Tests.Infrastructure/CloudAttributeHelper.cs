using Raven.Client.Documents.Operations.Backups;

namespace Tests.Infrastructure
{
    internal class CloudAttributeHelper
    {
        public static string TestIsMissingCloudCredentialEnvironmentVariable(bool envVariableMissing, string environmentVariable, string parsingError, BackupSettings settings)
        {
            if (RavenTestHelper.SkipIntegrationTests)
                return RavenTestHelper.SkipIntegrationMessage;

            if (RavenTestHelper.IsRunningOnCI)
                return null;

            if (envVariableMissing)
                return $"Test is missing '{environmentVariable}' environment variable.";

            if (string.IsNullOrEmpty(parsingError) == false)
                return $"Failed to parse the {nameof(BackupSettings)} settings, error: {parsingError}";

            if (settings == null)
                return $"Cloud backup tests missing {nameof(BackupSettings)}.";

            return null;
        }
    }
}
