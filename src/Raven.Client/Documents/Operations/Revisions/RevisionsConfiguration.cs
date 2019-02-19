//-----------------------------------------------------------------------
// <copyright file="RevisionsConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Operations.Revisions
{
    public class RevisionsConfiguration
    {
        public RevisionsCollectionConfiguration Default { get; set; }

        public Dictionary<string, RevisionsCollectionConfiguration> Collections { get; set; }

        public bool Equals(RevisionsConfiguration other)
        {
            if (other?.Default == null || other.Default.Equals(Default) == false)
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
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
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
                return hash ;
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
