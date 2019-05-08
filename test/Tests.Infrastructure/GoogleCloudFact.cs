using System;
using System.Runtime.CompilerServices;
using Xunit;

namespace Tests.Infrastructure
{
    class GoogleCloudFact : FactAttribute
    {
        private const string BucketNameEnvironmentVariable = "GOOGLE_CLOUD_BUCKET_NAME";
        private const string GoogleCloudCredentialEnvironmentVariable = "GOOGLE_CLOUD_CREDENTIAL";

        public static string BucketName { get;  private set; }

        public static string CredentialsJson { get; private set; }

        static GoogleCloudFact()
        {
            BucketName = Environment.GetEnvironmentVariable(BucketNameEnvironmentVariable, EnvironmentVariableTarget.User);
            CredentialsJson = Environment.GetEnvironmentVariable(GoogleCloudCredentialEnvironmentVariable, EnvironmentVariableTarget.User);
        }

        public GoogleCloudFact([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrWhiteSpace(BucketName))
            {
                Skip = $"Google cloud {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(CredentialsJson))
            {
                Skip = $"Google cloud {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }
        }
    }
}
