using System;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class GoogleCloudFact : FactAttribute
    {
        private const string BucketNameEnvironmentVariable = "GOOGLE_CLOUD_BUCKET_NAME";
        private const string GoogleCloudCredentialEnvironmentVariable = "GOOGLE_CLOUD_CREDENTIAL";

        public static GoogleCloudSettings GoogleCloudSettings { get; }

        static GoogleCloudFact()
        {
            GoogleCloudSettings = new GoogleCloudSettings
            {
                BucketName = Environment.GetEnvironmentVariable(BucketNameEnvironmentVariable, EnvironmentVariableTarget.User),
                GoogleCredentialsJson = Environment.GetEnvironmentVariable(GoogleCloudCredentialEnvironmentVariable, EnvironmentVariableTarget.User)
            };
        }

        public GoogleCloudFact([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.BucketName))
            {
                Skip = $"Google cloud {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(GoogleCloudSettings.GoogleCredentialsJson))
            {
                Skip = $"Google cloud {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }
        }
    }
}
