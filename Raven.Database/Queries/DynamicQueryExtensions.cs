using Raven.Database.Data;

namespace Raven.Database.Queries
{
    public static class DynamicQueryExtensions
    {
        public static QueryResult ExecuteDynamicQuery(this DocumentDatabase self, string entityName, IndexQuery indexQuery)
        {
            var dynamicQueryRunner = (DynamicQueryRunner)self.ExtensionsState.GetOrAdd(typeof (DynamicQueryExtensions), o => new DynamicQueryRunner(self));
            return dynamicQueryRunner.ExecuteDynamicQuery(entityName, indexQuery);
        }
    }
}