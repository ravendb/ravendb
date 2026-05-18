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

        private static readonly GlacierSettings _glacierSettings;

        public static GlacierSettings GlacierSettings => new GlacierSettings(_glacierSettings);

        private static readonly string ParsingError;

        static AmazonGlacierRetryTheoryAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.GlacierCredential == null)
                return;

            try
            {
                _glacierSettings = JsonConvert.DeserializeObject<GlacierSettings>(RavenTestHelper.EnvironmentVariables.GlacierCredential);
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
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(RavenTestHelper.EnvironmentVariables.GlacierCredential == null, RavenTestHelper.EnvironmentVariables.GlacierCredentialKey, ParsingError, _glacierSettings, skipIsRunningOnCI: true);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
