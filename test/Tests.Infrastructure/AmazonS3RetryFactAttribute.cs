using System;
using System.Runtime.CompilerServices;
using Newtonsoft.Json;
using Raven.Client.Documents.Operations.Backups;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class AmazonS3RetryFactAttribute : RetryFactAttribute, Xunit.v3.IFactAttribute
    {
        string Xunit.v3.IFactAttribute.Skip => this.Skip;

        private static readonly S3Settings _s3Settings;

        public static S3Settings S3Settings => _s3Settings == null ? null : new S3Settings(_s3Settings);

        private static readonly string ParsingError;

        static AmazonS3RetryFactAttribute()
        {
            if (RavenTestHelper.EnvironmentVariables.S3Credential == null)
                return;

            try
            {
                _s3Settings = JsonConvert.DeserializeObject<S3Settings>(RavenTestHelper.EnvironmentVariables.S3Credential);
            }
            catch (Exception e)
            {
                ParsingError = e.ToString();
            }
        }

        public AmazonS3RetryFactAttribute([CallerMemberName] string memberName = "", int maxRetries = 3, int delayBetweenRetriesMs = 0)
            : base(maxRetries, delayBetweenRetriesMs)
        {
        }

        public new string Skip
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
            skipMessage = CloudAttributeHelper.TestIsMissingCloudCredentialEnvironmentVariable(RavenTestHelper.EnvironmentVariables.S3Credential == null, RavenTestHelper.EnvironmentVariables.S3CredentialKey, ParsingError, _s3Settings);
            return string.IsNullOrEmpty(skipMessage) == false;
        }
    }
}
