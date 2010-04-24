using System;

namespace Raven.Database.Storage
{
    public class DocumentInTransactionData
    {
        public Guid Etag { get; set; }
        public bool Delete { get; set; }
        public byte[] Metadata { get; set; }
		public byte[] Data { get; set; }
        public string Key { get; set; }

    }
}