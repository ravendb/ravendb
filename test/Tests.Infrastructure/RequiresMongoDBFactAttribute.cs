using Tests.Infrastructure.ConnectionString;
using Xunit;

namespace Tests.Infrastructure
{
    public class RequiresMongoDBFactAttribute : FactAttribute
    {
        public RequiresMongoDBFactAttribute()
        {
            if (MongoDBConnectionString.Instance.CanConnect() == false)
                Skip = "Test requires MongoDB";
        }
    }
}
