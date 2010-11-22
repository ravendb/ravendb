namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System.Collections.Generic;
    using Management.Client.Silverlight;
    using Newtonsoft.Json.Linq;
    using Raven.Database;

    public class Document
    {
        private readonly IDictionary<string, JToken> data = new Dictionary<string, JToken>();
        private readonly IDictionary<string, JToken> metadata = new Dictionary<string, JToken>();
        private readonly JsonDocument jsonDocument;

        public Document(JsonDocument jsonDocument)
        {
            this.jsonDocument = jsonDocument;
            this.Id = jsonDocument.Key;
            this.data = ParseJsonToDictionary(jsonDocument.DataAsJson);
            this.metadata = ParseJsonToDictionary(jsonDocument.Metadata);
        }

        public string Id { get; set; }

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

        public JsonDocument JsonDocument
        {
            get { return jsonDocument; }
        }

        public void SetData(string json)
        {
            this.jsonDocument.DataAsJson = JObject.Parse(json);
        }

        public void SetMetadata(string json)
        {
            this.jsonDocument.Metadata = JObject.Parse(json);
        }

        public void Save(IAsyncDocumentSession session)
        {
            session.Store(this.jsonDocument);
            session.SaveChanges(saveResult => { });
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
