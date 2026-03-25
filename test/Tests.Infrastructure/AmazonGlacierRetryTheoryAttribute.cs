using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AmazonGlacierRetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

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

        public AmazonGlacierRetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
        }

        public new string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(base.Skip) == false)
                    return base.Skip;

                ShouldSkip(out var skipMessage);
                return skipMessage;
            }

            set => base.Skip = value;
        }

        private static bool ShouldSkip(out string skipMessage)
        {
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, GlacierCredentialEnvironmentVariable, ParsingError, _glacierSettings, skipIsRunningOnCI: true);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
