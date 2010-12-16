using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
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

        public static string FindDynamicIndexName(this DocumentDatabase self, string entityName, string query)
        {
            var map = DynamicQueryMapping.Create(self, query, entityName);

            return map.IndexName;
        }
    }
}