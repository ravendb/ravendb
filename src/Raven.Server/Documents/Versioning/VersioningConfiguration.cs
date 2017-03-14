//-----------------------------------------------------------------------
// <copyright file="VersioningConfiguration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Collections.Generic;

namespace Raven.Server.Documents.Versioning
{
#pragma warning disable CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
    public class VersioningConfiguration
#pragma warning restore CS0659 // Type overrides Object.Equals(object o) but does not override Object.GetHashCode()
    {
        public VersioningConfigurationCollection Default { get; set; }

        public Dictionary<string, VersioningConfigurationCollection> Collections { get; set; }

#pragma warning disable 659
        public override bool Equals(object obj)
#pragma warning restore 659
        {
            var other = obj as VersioningConfiguration;
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
    }
}