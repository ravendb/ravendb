//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Documents.Conventions;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevisionsConfiguration : IFillFromBlittableJson
    {
        public RevisionsCollectionConfiguration Default { get; set; }

        public Dictionary<string, RevisionsCollectionConfiguration> Collections { get; set; }

        public bool Equals(RevisionsConfiguration other)
        {
            if (other == null)
                return false;

            if (other.Default == null && Default != null)
                return false;

            if (other.Default != null && Default == null)
                return false;

            if (other.Default != null && other.Default.Equals(Default) == false)
                return false;

            foreach (var keyValue in Collections)
            {
                RevisionsCollectionConfiguration val;
                if (other.Collections.TryGetValue(keyValue.Key, out val) == false)
                    return false;
                if (keyValue.Value.Equals(val) == false)
                    return false;
            }
            foreach (var key in other.Collections.Keys)
            {
                if (Collections.ContainsKey(key) == false)
                    return false;
            }
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;
            if (ReferenceEquals(this, obj))
                return true;
            if (obj.GetType() != GetType())
                return false;
            return Equals((RevisionsConfiguration)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hash = Default?.GetHashCode() ?? 0;
                if (Collections == null)
                    return hash;

                foreach (var collection in Collections)
                {
                    hash = hash ^ (collection.Key.GetHashCode() * 397);
                    hash = hash ^ (collection.Value.GetHashCode() * 397);
                }
                return hash;
            }
        }

        public void FillFromBlittableJson(BlittableJsonReaderObject json)
        {
            var configuration = DocumentConventions.Default.Serialization.DefaultConverter.FromBlittable<RevisionsConfiguration>(json, "RevisionsConfiguration");
            Default = configuration.Default;
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>(StringComparer.OrdinalIgnoreCase);
            if (configuration.Collections == null)
                return;
            foreach (var collection in configuration.Collections)
            {
                Collections.Add(collection.Key, collection.Value);
            }
        }

        public DynamicJsonValue ToJson()
        {
            var collections = new DynamicJsonValue();

            foreach (var c in Collections)
            {
                collections[c.Key] = c.Value.ToJson();
            }

            return new DynamicJsonValue
            {
                [nameof(Default)] = Default?.ToJson(),
                [nameof(Collections)] = collections
            };
        }
    }
}
