using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Bundles.IndexReplicationToRedis.Data
{
    public class IndexReplicationToRedisDestination
    {
        public string Id { get; set; }

        public string Server { get; set; }

		public IndexReplicationToRedisMode RedisSaveMode { get; set; }
		public IList<string> FieldsToHashSave { get; set; }

		public string PocoTypeAssemblyQualifiedName { get; set; }


        public IndexReplicationToRedisDestination()
        {

        }
    }
}
