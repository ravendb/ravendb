using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure
{
    public class AmazonGlacierRetryFactAttribute : RetryFactAttribute
    {
        private const string GlacierCredentialEnvironmentVariable = "GLACIER_CREDENTIAL";

        private static readonly GlacierSettings _glacierSettings;

        public static GlacierSettings GlacierSettings => new GlacierSettings(_glacierSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AmazonGlacierRetryFactAttribute()
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

        public AmazonGlacierRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.IsRunningOnCI)
                return;

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
