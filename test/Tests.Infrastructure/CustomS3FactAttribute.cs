using System;
using System.IO;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using Xunit;

namespace Tests.Infrastructure
{
    public class CustomS3FactAttribute : FactAttribute
    {
        private const string S3CredentialEnvironmentVariable = "CUSTOM_S3_SETTINGS";

        public static S3Settings S3Settings { get; }

        private static readonly string ParsingError;

        static CustomS3FactAttribute()
        {
            var strSettings = Environment.GetEnvironmentVariable(S3CredentialEnvironmentVariable);

            if (string.IsNullOrEmpty(strSettings))
                return;

            try
            {
                S3Settings = JsonConvert.DeserializeObject<S3Settings>(strSettings);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public CustomS3FactAttribute([CallerMemberName] string memberName = "")
        {
            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse custom S3 settings, error: {ParsingError}";
                return;
            }

            if (S3Settings == null)
            {
                Skip = $"S3 {memberName} tests missing S3 settings.";
            }
        }
    }
}
