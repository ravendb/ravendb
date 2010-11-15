namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System;
    using System.Collections.Generic;
    using Client.Silverlight.Data;
    using Newtonsoft.Json.Linq;

    public class Document
    {
        private readonly string id;
        private readonly IDictionary<string, JToken> data = new Dictionary<string, JToken>();
        private readonly IDictionary<string, JToken> metadata = new Dictionary<string, JToken>();

        public Document(JsonDocument jsonDocument)
        {
            this.id = jsonDocument.Id;
            this.data = ParseJsonToDictionary(jsonDocument.DataAsJson);
            this.metadata = ParseJsonToDictionary(jsonDocument.Metadata);
        }

        public string Id
        {
            get { return this.id; }
        }

        public IDictionary<string, JToken> Data
        {
            get
            {
                return this.data;
            }
        }

        public IDictionary<string, JToken> Metadata
        {
            get
            {
                return this.metadata;
            }
        }

        private static IDictionary<string, JToken> ParseJsonToDictionary(JObject dataAsJson)
        {
            IDictionary<string, JToken> result = new Dictionary<string, JToken>();

            foreach (var d in dataAsJson)
            {
                result.Add(d.Key, d.Value);
            }

            return result;
        }
    }
}
