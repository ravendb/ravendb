using System;
using Tests.Infrastructure.ConnectionString;
using xRetry;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchTheoryAttribute : RetryTheoryAttribute
    {
        private static readonly bool _canConnect;

        static RequiresElasticSearchTheoryAttribute()
        {
            _canConnect = ElasticSearchTestNodes.Instance.CanConnect();
        }

        public RequiresElasticSearchTheoryAttribute(int maxRetries = 3,
            int delayBetweenRetriesMs = 1000,
            params Type[] skipOnExceptions) : base(maxRetries, delayBetweenRetriesMs, skipOnExceptions)
        {
            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}
