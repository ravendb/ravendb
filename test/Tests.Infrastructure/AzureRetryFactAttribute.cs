using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AzureRetryFactAttribute : RetryFactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        private static readonly AzureSettings _azureSettings;

        public static AzureSettings AzureSettings => new AzureSettings(_azureSettings);

        private static readonly string ParsingError;

        static AzureRetryFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.AzureCredential == null)
                return;

            try
            {
                _azureSettings = JsonConvert.DeserializeObject<AzureSettings>(RavenTestHelper.EnvironmentVariables.AzureCredential);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AzureRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
        }

        public new string Skip
        {
            get
            {
                return CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(RavenTestHelper.EnvironmentVariables.AzureCredential == null, RavenTestHelper.EnvironmentVariables.AzureCredentialKey, ParsingError, _azureSettings);
            }

            set => base.Skip = value;
        }

        public static bool ShouldSkip(out string skipMessage)
        {
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(RavenTestHelper.EnvironmentVariables.AzureCredential == null, RavenTestHelper.EnvironmentVariables.AzureCredentialKey, ParsingError, _azureSettings);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
