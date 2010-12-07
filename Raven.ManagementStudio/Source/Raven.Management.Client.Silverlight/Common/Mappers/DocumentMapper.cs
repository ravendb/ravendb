namespace Raven.Management.Client.Silverlight.Common.Mappers
{
    using System;
    using System.Globalization;
    using Database;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// 
    /// </summary>
    public class DocumentMapper : IMapper<JsonDocument>
    {
        #region IMapper<JsonDocument> Members

        /// <summary>
        /// 
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public JsonDocument Map(string json)
        {
            var document = new JsonDocument()
                               {
                                   DataAsJson = new JObject(),
                                   Metadata = new JObject()
                               };

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

                    JToken id = document.Metadata["@id"];
                    if (id != null)
                    {
                        document.Key = id.ToString().Replace("\"", string.Empty);
                        //document.Id = document.Key;
                    }

                    JToken etag = document.Metadata["@etag"];
                    if (etag != null)
                    {
                        document.Etag = new Guid(etag.ToString().Replace("\"", string.Empty));
                    }

                    JToken nonAuthoritiveInformation = document.Metadata["Non-Authoritive-Information"];
                    if (nonAuthoritiveInformation != null)
                    {
                        document.NonAuthoritiveInformation = Convert.ToBoolean(nonAuthoritiveInformation.ToString());
                    }

                    JToken lastModified = document.Metadata["Last-Modified"];
                    if (lastModified != null)
                    {
                        document.LastModified = DateTime.ParseExact(
                            lastModified.ToString().Replace("\"", string.Empty), "r", CultureInfo.InvariantCulture);
                    }
                }
            }

            return document;
        }

        #endregion
    }
}