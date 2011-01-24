namespace Raven.Client.Silverlight.Data
{
    using System;
    using Newtonsoft.Json.Linq;

    public class JsonDocument : Entity
    {
        public JObject DataAsJson { get; set; }

        public JObject Metadata { get; set; }

        public string Key { get; set; }

        public bool NonAuthoritiveInformation { get; set; }

        public Guid Etag { get; set; }

        public DateTime LastModified { get; set; }

        public JObject Projection { get; set; }

        public override JObject ToJson()
        {
            if (this.Projection != null)
            {
                return this.Projection;
            }

            var doc = new JObject();
            if (this.DataAsJson != null)
            {
                doc = new JObject(this.DataAsJson); // clone the document
            }

            var metadata = new JObject();
            if (this.Metadata != null)
            {
                metadata = new JObject(this.Metadata); // clone the metadata
            }

            metadata["Last-Modified"] = JToken.FromObject(this.LastModified.ToString("r"));
            var etagProp = metadata.Property("@etag");
            if (etagProp == null)
            {
                etagProp = new JProperty("@etag");
                metadata.Add(etagProp);
            }

            etagProp.Value = new JValue(this.Etag.ToString());
            doc.Add("@metadata", metadata);
            metadata["Non-Authoritive-Information"] = JToken.FromObject(this.NonAuthoritiveInformation);
            return doc;
        }
    }
}
