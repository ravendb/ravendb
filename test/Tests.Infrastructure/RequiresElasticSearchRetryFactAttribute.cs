using System;
using FastTests;
using Tests.Infrastructure.ConnectionString;
using xRetry;

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
            int delayBetweenRetriesMs = 1000,
            params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (RavenTestHelper.IsRunningOnCI)
                return;

            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}
