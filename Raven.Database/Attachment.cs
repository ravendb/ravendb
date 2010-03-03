using System;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
    public class Attachment
    {
        public byte[] Data { get; set; }
        public JObject Metadata { get; set; }
        public Guid Etag { get; set; }
    }
}