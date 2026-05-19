using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AmazonGlacierRetryFactAttribute : RetryFactAttribute
    {
        private static readonly GlacierSettings _glacierSettings;

        public static GlacierSettings GlacierSettings => new GlacierSettings(_glacierSettings);

        private static readonly string ParsingError;

        static AmazonGlacierRetryFactAttribute()
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

        public AmazonGlacierRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
            Skip = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(
                envVariableMissing: RavenTestHelper.EnvironmentVariables.GlacierCredential == null,
                environmentVariable: RavenTestHelper.EnvironmentVariables.GlacierCredentialEnvName,
                parsingError: ParsingError,
                settings: _glacierSettings,
                skipIsRunningOnCI: true);
        }
    }
}
