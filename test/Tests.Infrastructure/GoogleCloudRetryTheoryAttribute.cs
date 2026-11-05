using System.Runtime.CompilerServices;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class GoogleCloudRetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        public static GoogleCloudSettings GoogleCloudSettings => GoogleCloudRetryFactAttribute.GoogleCloudSettings;

        private readonly string _memberName;

        public GoogleCloudRetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
            _memberName = memberName;
        }

        public new string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(base.Skip) == false)
                    return base.Skip;

                if (ShouldSkip(_memberName, out var skipMessage))
                    return skipMessage;

                return base.Skip;
            }

            set => base.Skip = value;
        }

        public static bool ShouldSkip(string memberName, out string skipMessage)
        {
            skipMessage = null;

            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                skipMessage = RavenTestHelper.SkipIntegrationMessage;
                return true;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return false;

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.BucketName))
            {
                skipMessage = $"Google cloud {memberName} tests missing {RavenTestHelper.EnvironmentVariables.GoogleCloudBucketNameEnvName} environment variable.";
                return true;
            }

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.GoogleCredentialsJson))
            {
                skipMessage = $"Google cloud {memberName} tests missing {RavenTestHelper.EnvironmentVariables.GoogleCloudCredentialEnvName} environment variable.";
                return true;
            }

            return false;
        }
    }
}
