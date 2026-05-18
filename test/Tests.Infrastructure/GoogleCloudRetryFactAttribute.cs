using System.Runtime.CompilerServices;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class GoogleCloudRetryFactAttribute : RetryFactAttribute
    {
        public static GoogleCloudSettings GoogleCloudSettings { get; }

        static GoogleCloudRetryFactAttribute()
        {
            GoogleCloudSettings = new GoogleCloudSettings
            {
                BucketName = RavenTestHelper.EnvironmentVariables.GoogleCloudBucketName,
                GoogleCredentialsJson = RavenTestHelper.EnvironmentVariables.GoogleCloudCredential
            };
        }

        public GoogleCloudRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
            if (RavenTestHelper.EnvironmentVariables.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
                return;

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.BucketName))
            {
                Skip = $"Google cloud {memberName} tests missing {RavenTestHelper.EnvironmentVariables.GoogleCloudBucketNameEnvName} environment variable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.GoogleCredentialsJson))
            {
                Skip = $"Google cloud {memberName} tests missing {RavenTestHelper.EnvironmentVariables.GoogleCloudCredentialEnvName} environment variable.";
                return;
            }
        }
    }
}
