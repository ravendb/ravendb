using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class AmazonGlacierFactAttribute : FactAttribute
    {
        private const string GlacierCredentialEnvironmentVariable = "GLACIER_CREDENTIAL";

        private static readonly GlacierSettings _glacierSettings;

        public static GlacierSettings GlacierSettings => new GlacierSettings(_glacierSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AmazonGlacierFactAttribute()
        {
            var glacierSettingsString = Environment.GetEnvironmentVariable(GlacierCredentialEnvironmentVariable);
            if (glacierSettingsString == null)
            {
                EnvVariableMissing = true;
                return;
            }

            try
            {
                _glacierSettings = JsonConvert.DeserializeObject<GlacierSettings>(glacierSettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AmazonGlacierFactAttribute([CallerMemberName] string memberName = "")
        {
            if (EnvVariableMissing)
            {
                Skip = $"Test is missing '{GlacierCredentialEnvironmentVariable}' environment variable.";
                return;
            }

            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse the Amazon Glacier settings, error: {ParsingError}";
                return;
            }

            if (_glacierSettings == null)
            {
                Skip = $"Glacier {memberName} tests missing Amazon Glacier settings.";
                return;
            }
        }
    }
}
