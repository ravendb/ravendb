using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class S3Fact : FactAttribute
    {
        private const string S3CredentialEnvironmentVariable = "S3_CREDENTIAL";

        public static S3Settings S3Settings { get; private set; }

        private static readonly string ParsingError;

        static S3Fact()
         {
            var s3SettingsString = Environment.GetEnvironmentVariable(S3CredentialEnvironmentVariable, EnvironmentVariableTarget.User);

            try
            {
                S3Settings = JsonConvert.DeserializeObject<S3Settings>(s3SettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public S3Fact([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse S3 settings, error: {ParsingError}";
                return;
            }

            if (S3Settings == null)
            {
                Skip = $"Google cloud {memberName} tests missing BucketNameEnvironmentVariable.";
                return;
            }
        }
    }
}
