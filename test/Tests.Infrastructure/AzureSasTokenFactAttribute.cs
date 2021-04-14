using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class AzureSasTokenFactAttribute : FactAttribute
    {
        private const string AzureCredentialEnvironmentVariable = "AZURE_SAS_TOKEN_CREDENTIAL";

        public static AzureSettings AzureSettings { get; }

        private static readonly string ParsingError;

        static AzureSasTokenFactAttribute()
        {
            var azureSettingsString = Environment.GetEnvironmentVariable(AzureCredentialEnvironmentVariable);

            try
            {
                AzureSettings = JsonConvert.DeserializeObject<AzureSettings>(azureSettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AzureSasTokenFactAttribute([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse the Azure settings, error: {ParsingError}";
                return;
            }

            if (AzureSettings == null)
            {
                Skip = $"S3 {memberName} tests missing Azure settings.";
                return;
            }
        }
    }
}
