using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database.Storage
{
    public class DocumentInTransactionData
    {
        public Guid Etag { get; set; }
        public bool Delete { get; set; }
        public JObject Metadata { get; set; }
		public JObject Data { get; set; }
        public string Key { get; set; }

    }
}