using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Sparrow.Json;

namespace Raven.Client.Indexing
{
    public class IndexConfiguration : Dictionary<string, string>, IFillFromBlittableJson
    {
        /// <summary>
        /// Index specific setting that limits the number of map outputs that an index is allowed to create for a one source document. If a map operation applied to
        /// the one document produces more outputs than this number then an index definition will be considered as a suspicious, the indexing of this document 
        /// will be skipped and the appropriate error message will be added to the indexing errors.
        /// <para>Default value: null means that the global value from Raven configuration will be taken to detect if number of outputs was exceeded.</para>
        /// </summary>
        [JsonIgnore]
        public int? MaxIndexOutputsPerDocument
        {
            get
            {
                var value = GetValue(Constants.Configuration.Indexing.MaxIndexOutputsPerDocument);
                if (value == null)
                    return null;

                int valueAsInt;
                if (int.TryParse(value, out valueAsInt) == false)
                    return null;

                return valueAsInt;
            }

            set
            {
                Add(Constants.Configuration.Indexing.MaxIndexOutputsPerDocument, value?.ToInvariantString());
            }
        }

        public new void Add(string key, string value)
        {
            if (string.Equals(key, Constants.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument, StringComparison.OrdinalIgnoreCase)
                || string.Equals(key, Constants.Configuration.Indexing.MaxMapReduceIndexOutputsPerDocument, StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException($"Cannot set '{key}' key. Use '{Constants.Configuration.Indexing.MaxIndexOutputsPerDocument}' instead.");

            base[key] = value;
        }

        public new string this[string key]
        {
            get { return base[key]; }
            set { Add(key, value);}
        }

        public string GetValue(string key)
        {
            string value;
            if (TryGetValue(key, out value) == false)
                return null;

            return value;
        }

        public bool Equals(IndexConfiguration configuration, bool ignoreMaxIndexOutputs)
        {
            if (configuration == null)
                return false;

            if (Count != configuration.Count)
                return false;

            foreach (var kvp in this)
            {
                if (ignoreMaxIndexOutputs && kvp.Key.Equals(Constants.Configuration.Indexing.MaxIndexOutputsPerDocument, StringComparison.OrdinalIgnoreCase))
                    continue;

                string value;
                if (configuration.TryGetValue(kvp.Key, out value) == false)
                    return false;

                if (Equals(value, kvp.Value) == false)
                    return false;
            }

            return true;
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
                this[propertyName] = json[propertyName]?.ToString();
        }
    }
}