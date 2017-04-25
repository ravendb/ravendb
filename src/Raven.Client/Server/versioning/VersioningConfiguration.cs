//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Server.Documents.Versioning
{
    public class VersioningConfiguration
    {
        public VersioningConfigurationCollection Default { get; set; }

        public Dictionary<string, VersioningConfigurationCollection> Collections { get; set; }

        public bool Equals(VersioningConfiguration other)
        {
            if (other == null)
                return false;
            if (other.Default.Equals(Default) == false)
                return false;
            foreach (var keyValue in Collections)
            {
                VersioningConfigurationCollection val;
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
            if (obj.GetType() != this.GetType()) return false;
            return Equals((VersioningConfiguration)obj);
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
    }
}