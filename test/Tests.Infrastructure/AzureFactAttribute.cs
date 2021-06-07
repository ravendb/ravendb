using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class AzureFactAttribute : FactAttribute
    {
        private const string AzureCredentialEnvironmentVariable = "AZURE_CREDENTIAL";

        private static readonly AzureSettings _azureSettings;

        public static AzureSettings AzureSettings => new AzureSettings(_azureSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AzureFactAttribute()
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

        public AzureFactAttribute([CallerMemberName] string memberName = "")
        {
            if (EnvVariableMissing)
            {
                Skip = $"Test is missing '{AzureCredentialEnvironmentVariable}' environment variable.";
                return;
            }

            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse the Azure settings, error: {ParsingError}";
                return;
            }

            if (_azureSettings == null)
            {
                Skip = $"Azure {memberName} tests missing {nameof(AzureSettings)}.";
                return;
            }
        }
    }
}
