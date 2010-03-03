using System;
using System.Text;
using Newtonsoft.Json.Linq;

namespace Raven.Database
{
    public class JsonDocument
    {
        public byte[] Data { get; set; }
        public JObject Metadata { get; set; }
        public string Key { get; set; }
        public Guid Etag { get; set; }

        public JObject ToJson()
        {
            var doc = JObject.Parse(Encoding.UTF8.GetString(Data));
            var etagProp = Metadata.Property("@etag");
            if(etagProp == null)
            {
                etagProp = new JProperty("etag");
                Metadata.Add(etagProp);
            }
            etagProp.Value = new JValue(Etag.ToString());
            doc.Add("@metadata", Metadata);
            return doc;
        }
    }
}