//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication
{
    /// <summary>
    /// Data class for replication destination documents
    /// </summary>
    public class ReplicationNode : IEquatable<ReplicationNode>, IComparable<ReplicationNode>
    {
        /// <summary>
        /// The name of the node 
        /// </summary>
        public string NodeTag;

        /// <summary>
        /// The name of the connection string specified in the 
        /// server configuration file. 
        /// Override all other properties of the destination
        /// </summary>

        private string _url;

        /// <summary>
        /// Gets or sets the URL of the replication destination
        /// </summary>
        /// <value>The URL.</value>
        public string Url
        {
            get => _url;
            set => _url = value == null ? null : value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
        }

        /// <summary>
        /// The database to use
        /// </summary>
        public string Database { get; set; }
        
        /// <summary>
        /// Gets or sets if the replication will ignore this destination in the client
        /// </summary>
        public bool IgnoredClient { get; set; }

        /// <summary>
        /// Gets or sets if replication to this destination is disabled in both client and server.
        /// </summary>
        public bool Disabled { get; set; }

        /// <summary>
        /// Gets or sets the Client URL of the replication destination
        /// </summary>
        public string ClientVisibleUrl { get; set; }

        /// <summary>
        /// If not null then only docs from specified collections are replicated and transformed / filtered according to an optional script.
        /// </summary>
        public Dictionary<string, string> SpecifiedCollections { get; set; }

        public string Humane
        {
            get
            {
                if (string.IsNullOrEmpty(_url))
                    return null;
                return _url + " " + NodeTag;
            }
        }

        public bool CanBeFailover() =>
            IgnoredClient == false && Disabled == false && (SpecifiedCollections == null || SpecifiedCollections.Count == 0);

        public override string ToString() => $"{nameof(Url)}: {Url}, {nameof(NodeTag)}: {NodeTag}";

        public bool Equals(ReplicationNode other) => IsEqualTo(other);

        public bool IsMatch(ReplicationNode other)
        {
            return
                string.Equals(Url, other.Url, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(NodeTag, other.NodeTag, StringComparison.OrdinalIgnoreCase);
        }

        public bool IsEqualTo(ReplicationNode other)
        {
            return string.Equals(NodeTag, other.NodeTag, StringComparison.OrdinalIgnoreCase) &&
                   string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase) &&
                   IgnoredClient.Equals(other.IgnoredClient) && Disabled.Equals(other.Disabled) &&
                   ((string.Equals(Url, other.Url, StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(ClientVisibleUrl)) ||
                   (!string.IsNullOrWhiteSpace(ClientVisibleUrl) && string.Equals(ClientVisibleUrl, other.ClientVisibleUrl, StringComparison.OrdinalIgnoreCase))) &&
                   DictionaryExtensions.ContentEquals(SpecifiedCollections, other.SpecifiedCollections);
        }

        public int CompareTo(ReplicationNode other)
        {
            var rc = string.Compare(NodeTag ?? Url, other.NodeTag ?? Url, StringComparison.OrdinalIgnoreCase);
            if (rc != 0)
                return rc;
            
            return string.Compare(Database, other.Database, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ReplicationNode)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (NodeTag?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Database?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (ClientVisibleUrl?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public virtual ulong GetTaskKey()
        {
            var hashCode = CalculateStringHash(NodeTag);
            hashCode = (hashCode * 397) ^ CalculateStringHash(Database);
            hashCode = (hashCode * 397) ^ CalculateStringHash(ClientVisibleUrl);
            return hashCode;
        }

        protected static ulong CalculateStringHash(string s)
        {
            return string.IsNullOrEmpty(s) ? 0 : Hashing.XXHash64.Calculate(s, Encodings.Utf8);
        }

        public virtual DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(ClientVisibleUrl)] = ClientVisibleUrl,
                [nameof(Database)] = Database,
                [nameof(NodeTag)] = NodeTag,
                [nameof(Disabled)] = Disabled,
                [nameof(Humane)] = Humane,
                [nameof(IgnoredClient)] = IgnoredClient,
                [nameof(Url)] = Url,
            };

            if (SpecifiedCollections != null)
            {
                var values = new DynamicJsonValue();
                foreach (var kvp in SpecifiedCollections)
                    values[kvp.Key] = kvp.Value;

                json[nameof(SpecifiedCollections)] = values;
            }

            return json;
        }
    }
}