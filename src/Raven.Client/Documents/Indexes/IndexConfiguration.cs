using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Indexes
{
    public class IndexConfiguration : Dictionary<string, string>, IFillFromBlittableJson
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

        public string GetValue(string key)
        {
            string value;
            if (TryGetValue(key, out value) == false)
                return null;

            return value;
        }

        protected bool Equals(IndexConfiguration other)
        {
            return DictionaryExtensions.ContentEquals(this, other);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((IndexConfiguration)obj);
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