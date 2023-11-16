using System;
using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure
{
    public class GoogleCloudRetryFactAttribute : RetryFactAttribute
    {
        private const string BucketNameEnvironmentVariable = "GOOGLE_CLOUD_BUCKET_NAME";
        private const string GoogleCloudCredentialEnvironmentVariable = "GOOGLE_CLOUD_CREDENTIAL";

        public static GoogleCloudSettings GoogleCloudSettings { get; }

        static GoogleCloudRetryFactAttribute()
        {
            GoogleCloudSettings = new GoogleCloudSettings
            {
                BucketName = Environment.GetEnvironmentVariable(BucketNameEnvironmentVariable),
                GoogleCredentialsJson = Environment.GetEnvironmentVariable(GoogleCloudCredentialEnvironmentVariable)
            };
        }

        public GoogleCloudRetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.BucketName))
            {
                Skip = $"Google cloud {memberName} tests missing {BucketNameEnvironmentVariable} environment variable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.GoogleCredentialsJson))
            {
                Skip = $"Google cloud {memberName} tests missing {BucketNameEnvironmentVariable} environment variable.";
                return;
            }
        }
    }
}
