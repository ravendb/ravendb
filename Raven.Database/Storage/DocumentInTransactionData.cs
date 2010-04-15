using System;

namespace Raven.Database.Storage
{
    public class DocumentInTransactionData
    {
        public Guid Etag { get; set; }
        public bool Delete { get; set; }
        public string Metadata { get; set; }
        public string Data { get; set; }
        public string Key { get; set; }

    }
}