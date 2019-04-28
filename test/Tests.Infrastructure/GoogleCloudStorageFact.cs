using System;
using System.Runtime.CompilerServices;
using Xunit;

namespace Tests.Infrastructure
{
    class GoogleCloudStorageFact : FactAttribute
    {
        private const string BucketNameEnvironmentVariable = "GOOGLE_CLOUD_STORAGE_BUCKET_NAME";
        private const string GoogleCloudStorageCredentialEnvironmentVariable = "GOOGLE_CLOUD_STORAGE_CREDENTIAL";

        public static string BucketName { get;  private set; }

        public static string CredentialsJson { get; private set; }

        static GoogleCloudStorageFact()
        {
            BucketName = Environment.GetEnvironmentVariable(BucketNameEnvironmentVariable, EnvironmentVariableTarget.User);
            CredentialsJson = Environment.GetEnvironmentVariable(GoogleCloudStorageCredentialEnvironmentVariable, EnvironmentVariableTarget.User);
        }

        public GoogleCloudStorageFact([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrWhiteSpace(BucketName))
            {
                Skip = $"Google cloud storage {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }

            if (string.IsNullOrWhiteSpace(CredentialsJson))
            {
                Skip = $"Google cloud storage {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }
        }
    }
}
