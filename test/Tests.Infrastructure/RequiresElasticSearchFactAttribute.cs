using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchFactAttribute : RetryFactAttribute
    {
        private static readonly bool _canConnect;

        static RequiresElasticSearchFactAttribute()
        {
            _canConnect = ElasticSearchTestNodes.Instance.CanConnect();
        }

        public RequiresElasticSearchFactAttribute(int maxRetries = 3,
            int delayBetweenRetriesMs = 1000,
            params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}
