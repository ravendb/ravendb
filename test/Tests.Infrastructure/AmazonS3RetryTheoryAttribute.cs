using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AmazonS3RetryTheoryAttribute : RetryTheoryAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

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

        public AmazonS3RetryTheoryAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
        }

        public new string Skip
        {
            get
            {
                if (string.IsNullOrEmpty(base.Skip) == false)
                    return base.Skip;

                if (ShouldSkip(out var skipMessage))
                    return skipMessage;

                return base.Skip;
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
