namespace Raven.Client.Silverlight.Common.Mappers
{
    using System;
    using System.Globalization;
    using Newtonsoft.Json.Linq;
    using Raven.Client.Silverlight.Data;

    public class DocumentMapper : IMapper<JsonDocument>
    {
        public JsonDocument Map(string json)
        {
            var document = new JsonDocument();

            if (!string.IsNullOrEmpty(json))
            {
                document.DataAsJson = JObject.Parse(json);

                if (document.DataAsJson["@metadata"] != null)
                {
                    document.Metadata = JObject.Parse(document.DataAsJson["@metadata"].ToString());
                }

                if (document.Metadata != null)
                {
                    document.DataAsJson.Remove("@metadata");

                    var id = document.Metadata["@id"];
                    if (id != null)
                    {
                        document.Key = id.ToString().Replace("\"", string.Empty);
                        document.Id = document.Key;
                    }

                    var etag = document.Metadata["@etag"];
                    if (etag != null)
                    {
                        document.Etag = new Guid(etag.ToString().Replace("\"", string.Empty));
                    }

                    var nonAuthoritiveInformation = document.Metadata["Non-Authoritive-Information"];
                    if (nonAuthoritiveInformation != null)
                    {
                        document.NonAuthoritiveInformation = Convert.ToBoolean(nonAuthoritiveInformation.ToString());
                    }

                    var lastModified = document.Metadata["Last-Modified"];
                    if (lastModified != null)
                    {
                        document.LastModified = DateTime.ParseExact(
                            lastModified.ToString().Replace("\"", string.Empty), "r", CultureInfo.InvariantCulture);
                    }
                }
            }

            return document;
        }
    }
}
