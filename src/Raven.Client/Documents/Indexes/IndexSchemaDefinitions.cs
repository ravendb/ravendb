using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Indexes
{
    public sealed class IndexSchemaDefinitions : Dictionary<string, string>, IFillFromBlittableJson
    {
        public new void Add(string key, string value)
        {
            base[key] = value;
        }

        public new string this[string key]
        {
            get => base[key];
            set => Add(key, value);
        }
        
        private bool Equals(IndexSchemaDefinitions other)
        {
            return DictionaryExtensions.ContentEquals(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((IndexSchemaDefinitions)obj);
        }

        public override int GetHashCode()
        {
            return Count;
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            if (json == null)
                return;

            foreach (var propertyName in json.GetPropertyNames())
                this[propertyName] = json[propertyName].ToString();
        }
    }
}
