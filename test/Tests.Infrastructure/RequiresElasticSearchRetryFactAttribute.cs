using System;
using Tests.Infrastructure.ConnectionString;
using xRetry.v3;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchRetryFactAttribute : RetryFactAttribute
    {
        private static readonly bool _canConnect;

        static RequiresElasticSearchRetryFactAttribute()
        {
            _canConnect = ElasticSearchTestNodes.Instance.CanConnect;
        }

        public RequiresElasticSearchRetryFactAttribute(int maxRetries = 3,
            int delayBetweenRetriesMs = 1000) : base(maxRetries, delayBetweenRetriesMs)
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                Skip = RavenTestHelper.SkipIntegrationMessage;
                return;
            }

            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }

        public static bool ShouldSkip(out string skipMessage)
        {
            if (RavenTestHelper.SkipIntegrationTests)
            {
                skipMessage = RavenTestHelper.SkipIntegrationMessage;
                return true;
            }

            if (RavenTestHelper.IsRunningOnCI)
            {
                skipMessage = null;
                return false;
            }

            if (_canConnect == false)
            {
                skipMessage = "Test requires ElasticSearch instance";
                return true;
            }

            skipMessage = null;
            return false;
        }
    }
}
