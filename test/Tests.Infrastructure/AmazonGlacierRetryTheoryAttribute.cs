using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure
{
    public class AmazonGlacierRetryTheoryAttribute : RetryTheoryAttribute
    {
        private const string GlacierCredentialEnvironmentVariable = "GLACIER_CREDENTIAL";

        private static readonly GlacierSettings _glacierSettings;

        public static GlacierSettings GlacierSettings => new GlacierSettings(_glacierSettings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AmazonGlacierRetryTheoryAttribute()
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

        public AmazonGlacierRetryTheoryAttribute()
        {
        }

        public AmazonGlacierRetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
        }

        public override string Skip
        {
            get
            {
                return AzureRetryTheoryAttribute.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, GlacierCredentialEnvironmentVariable, ParsingError, _glacierSettings, skipIsRunningOnCICheck: true);
            }

            set => base.Skip = value;
        }
    }
}
