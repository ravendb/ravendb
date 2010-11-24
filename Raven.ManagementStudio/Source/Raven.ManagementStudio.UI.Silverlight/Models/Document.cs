namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System;
    using System.Collections.Generic;
    using Management.Client.Silverlight;
    using Management.Client.Silverlight.Common;
    using Newtonsoft.Json.Linq;
    using Raven.Database;
    using Newtonsoft.Json;

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

        public string Id { get; private set; }

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

        public static string ParseExceptionMessage { get; set; }

        public JsonDocument JsonDocument
        {
            get { return this.jsonDocument; }
        }

        public void SetData(string json)
        {
            this.jsonDocument.DataAsJson = JObject.Parse(json);     
        }

        public void SetMetadata(string json)
        {
            this.jsonDocument.Metadata = JObject.Parse(json);
        }

        public void SetId(string id)
        {
            this.jsonDocument.Key = id;
        }

        public void Save(IAsyncDocumentSession session, CallbackFunction.SaveMany<object> callback)
        {
            session.Store(this.jsonDocument);
            session.SaveChanges(callback);
        }

        public static bool ValidateJson(string json)
        {
            try
            {
                JObject.Parse(json);
                ParseExceptionMessage = string.Empty;
                return true;
            }
            catch (JsonReaderException exception)
            {
                ParseExceptionMessage = exception.Message;
                return false;
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
