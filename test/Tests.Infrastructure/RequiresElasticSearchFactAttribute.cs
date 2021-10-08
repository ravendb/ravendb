using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresElasticSearchFactAttribute : FactAttribute
    {
        public RequiresElasticSearchFactAttribute()
        {
            if (ElasticSearchTestNodes.Instance.CanConnect() == false)
                Skip = "Test requires ElasticSearch instance";
        }
    }
}
