using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AzureSasTokenRetryFactAttribute : RetryFactAttribute
    {
        private static readonly AzureSettings _azureSettings;

        public static AzureSettings AzureSettings => new AzureSettings(_azureSettings);

        private static readonly string ParsingError;

        static AzureSasTokenRetryFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.AzureSasTokenCredential == null)
                return;

            try
            {
                _azureSettings = JsonConvert.DeserializeObject<AzureSettings>(RavenTestHelper.EnvironmentVariables.AzureSasTokenCredential);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AzureSasTokenRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
            Skip = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(
                envVariableMissing: RavenTestHelper.EnvironmentVariables.AzureSasTokenCredential == null,
                environmentVariable: RavenTestHelper.EnvironmentVariables.AzureSasTokenCredentialKey,
                parsingError: ParsingError,
                settings: _azureSettings);
        }
    }
}
