using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class CustomS3RetryFactAttribute : RetryFactAttribute
    {
        private static readonly S3Settings _s3Settings;

        public static S3Settings S3Settings => new S3Settings(_s3Settings);

        private static readonly string ParsingError;

        static CustomS3RetryFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.CustomS3Settings == null)
                return;

            if (string.IsNullOrEmpty(RavenTestHelper.EnvironmentVariables.CustomS3Settings))
                return;

            try
            {
                _s3Settings = JsonConvert.DeserializeObject<S3Settings>(RavenTestHelper.EnvironmentVariables.CustomS3Settings);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public CustomS3RetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
            //if (RavenTestHelper.EnvironmentVariables.IsRunningOnCI)
            //    return;

            if (RavenTestHelper.EnvironmentVariables.CustomS3Settings == null)
            {
                Skip = $"Test is missing '{RavenTestHelper.EnvironmentVariables.CustomS3SettingsKey}' environment variable.";
                return;
            }

            if (string.IsNullOrEmpty(ParsingError) == false)
            {
                Skip = $"Failed to parse custom S3 settings, error: {ParsingError}";
                return;
            }

            if (_s3Settings == null)
            {
                Skip = $"S3 {memberName} tests missing S3 settings.";
            }
        }
    }
}
