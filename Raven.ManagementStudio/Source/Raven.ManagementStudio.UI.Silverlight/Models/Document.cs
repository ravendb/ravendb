namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System;
    using System.Collections.Generic;
    using Client;
    using Newtonsoft.Json.Linq;
    using Raven.Database;
    using Newtonsoft.Json;

    public class Document
    {
        private readonly IDictionary<string, JToken> _data;
        private readonly IDictionary<string, JToken> _metadata;
        private readonly JsonDocument _jsonDocument;

        public Document(JsonDocument jsonDocument)
        {
            _data = new Dictionary<string, JToken>();
            _metadata = new Dictionary<string, JToken>();
            
            _jsonDocument = jsonDocument;
            Id = jsonDocument.Key;
            _data = ParseJsonToDictionary(jsonDocument.DataAsJson);
            _metadata = ParseJsonToDictionary(jsonDocument.Metadata);
        }

        public string Id { get; private set; }

        public IDictionary<string, JToken> Data
        {
            get
            {
                return _data;
            }
        }

        public IDictionary<string, JToken> Metadata
        {
            get
            {
                return _metadata;
            }
        }

        public static string ParseExceptionMessage { get; set; }

        public JsonDocument JsonDocument
        {
            get { return _jsonDocument; }
        }

        public void SetData(string json)
        {
            _jsonDocument.DataAsJson = JObject.Parse(json);     
        }

        public void SetMetadata(string json)
        {
            _jsonDocument.Metadata = JObject.Parse(json);
        }

        public void SetId(string id)
        {
            _jsonDocument.Key = id;
        }

        public void Save(IAsyncDocumentSession session, CallbackFunction.SaveMany<object> callback)
        {
            session.Store(_jsonDocument);
            session.SaveChangesAsync();
            Id = _jsonDocument.Key;
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
