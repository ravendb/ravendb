using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry;

namespace Tests.Infrastructure
{
    public class AmazonS3RetryTheoryAttribute : RetryTheoryAttribute
    {
        private const string S3CredentialEnvironmentVariable = "S3_CREDENTIAL";

        private static readonly S3Settings _s3Settings;

        public static S3Settings S3Settings => new S3Settings(_s3Settings);

        private static readonly string ParsingError;

        private static readonly bool EnvVariableMissing;

        static AmazonS3RetryTheoryAttribute()
        {
            var s3SettingsString = Environment.GetEnvironmentVariable(S3CredentialEnvironmentVariable);
            if (s3SettingsString == null)
            {
                EnvVariableMissing = true;
                return;
            }

            try
            {
                _s3Settings = JsonConvert.DeserializeObject<S3Settings>(s3SettingsString);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AmazonS3RetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0, params Type[] skipOnExceptions)
            : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
        }

        public override string Skip
        {
            get
            {
                ShouldSkip(out var skipMessage);
                return skipMessage;
            }

            set => base.Skip = value;
        }

        public static bool ShouldSkip(out string skipMessage)
        {
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(EnvVariableMissing, S3CredentialEnvironmentVariable, ParsingError, _s3Settings);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
