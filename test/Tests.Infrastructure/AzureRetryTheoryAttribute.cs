using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure
{
    public class AzureRetryTheoryAttribute : RetryTheoryAttribute
    {
        private const string AzureCredentialEnvironmentVariable = "AZURE_CREDENTIAL";

        private static readonly AzureSettings _azureSettings;

        public static AzureSettings AzureSettings => new AzureSettings(_azureSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AzureRetryTheoryAttribute()
        {
            var azureSettingsString = Environment.GetEnvironmentVariable(AzureCredentialEnvironmentVariable);
            if (azureSettingsString == null)
            {
                EnvVariableMissing = true;
                return;
            }

            try
            {
                _azureSettings = JsonConvert.DeserializeObject<AzureSettings>(azureSettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AzureRetryTheoryAttribute()
        {
        }

        public AzureRetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
        }


        public override string Skip
        {
            get
            {
                return TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, AzureCredentialEnvironmentVariable, ParsingError, _azureSettings);
            }

            set => base.Skip = value;
        }

        // ReSharper disable once InconsistentNaming
        public static string TestIsMissingCloudCredentialEnvironmentVariable(bool envVariableMissing, string environmentVariable, string parsingError, BackupSettings settings, bool skipIsRunningOnCICheck = false)
        {
            if (RavenTestHelper.SkipIntegrationTests)
                return RavenTestHelper.SkipIntegrationMessage;

            if (skipIsRunningOnCICheck == false && RavenTestHelper.IsRunningOnCI)
                return null;

            if (envVariableMissing)
                return $"Test is missing '{environmentVariable}' environment variable.";

            if (string.IsNullOrEmpty(parsingError) == false)
                return $"Failed to parse the {nameof(BackupSettings)}, error: {parsingError}";

            if (settings == null)
                return $"Cloud backup tests missing {nameof(BackupSettings)}.";

            return null;
        }
    }
}
