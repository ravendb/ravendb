using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchFactAttribute : FactAttribute
    {
        private static readonly bool _canConnect;

        static RequiresElasticSearchFactAttribute()
        {
            _canConnect = ElasticSearchTestNodes.Instance.CanConnect();
        }

        public RequiresElasticSearchFactAttribute()
        {
            if (_canConnect == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}
