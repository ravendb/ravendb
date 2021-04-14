using System;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class AzureTheoryAttribute : TheoryAttribute
    {
        private const string AzureCredentialEnvironmentVariable = "AZURE_CREDENTIAL";

        private static readonly AzureSettings _azureSettings;

        public static AzureSettings AzureSettings => new AzureSettings(_azureSettings);

        private static readonly string ParsingError;

        static AzureTheoryAttribute()
        {
            var azureSettingsString = Environment.GetEnvironmentVariable(AzureCredentialEnvironmentVariable);

            try
            {
                _azureSettings = JsonConvert.DeserializeObject<AzureSettings>(azureSettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public override string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(ParsingError) == false)
                    return $"Failed to parse the Azure settings, error: {ParsingError}";

                if (AzureSettings == null)
                    return $"Azure tests missing {nameof(AzureSettings)}.";

                return null;
            }
        }
    }
}
