using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AzureRetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

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

        public AzureRetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
        }

        public new string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(base.Skip) == false)
                    return base.Skip;

                if (ShouldSkip(out var skipMessage))
                    return skipMessage;

                return base.Skip;
            }

            set => base.Skip = value;
        }

        public static bool ShouldSkip(out string skipMessage)
        {
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, AzureCredentialEnvironmentVariable, ParsingError, _azureSettings);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
