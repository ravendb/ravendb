using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresMongodbFactAttribute : FactAttribute
    {
        public RequiresMongodbFactAttribute()
        {
            if (MongodbConnectionString.Instance.CanConnect() == false)
                Skip = "Test requires Mongodb";
        }
    }
}
