using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class AmazonS3FactAttribute : FactAttribute
    {
        private const string S3CredentialEnvironmentVariable = "S3_CREDENTIAL";

        public static S3Settings S3Settings { get; }

        private static readonly string ParsingError;

        static AmazonS3FactAttribute()
        {
            var s3SettingsString = Environment.GetEnvironmentVariable(S3CredentialEnvironmentVariable);

            try
            {
                S3Settings = JsonConvert.DeserializeObject<S3Settings>(s3SettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AmazonS3FactAttribute([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse the Amazon S3 settings, error: {ParsingError}";
                return;
            }

            if (S3Settings == null)
            {
                Skip = $"S3 {memberName} tests missing Amazon S3 settings.";
                return;
            }
        }
    }
}
