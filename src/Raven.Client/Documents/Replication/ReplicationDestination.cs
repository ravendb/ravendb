//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using Raven.Client.Extensions;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication
{
    /// <summary>
    /// Data class for replication destination documents
    /// </summary>
    public class ReplicationDestination : IEquatable<ReplicationDestination>
    {
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
            get { return _url; }
            set
            {
                _url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
            }
        }

        /// <summary>
        /// The replication server username to use
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// The replication server password to use
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// The replication server domain to use
        /// </summary>
        public string Domain { get; set; }

        /// <summary>
        /// The database to use
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// How should the replication bundle behave with respect to replicated documents.
        /// If a document was replicated to us from another node, should we replicate that to
        /// this destination, or should we replicate only documents that were locally modified.
        /// </summary>
        public TransitiveReplicationOptions TransitiveReplicationBehavior { get; set; }

        /// <summary>
        /// Gets or sets a flag that controls whether index is replicated to the node or not
        /// </summary>
        public bool SkipIndexReplication { get; set; }

        /// <summary>
        /// Gets or sets if the replication will ignore this destination in the client
        /// </summary>
        public bool IgnoredClient { get; set; }

        /// <summary>
        /// Gets or sets if replication to this destination is disabled in both client and server.
        /// </summary>
        public bool Disabled { get; set; }

        public string AuthenticationScheme { get; set; }

        public string ApiKey; // TODO: remove me

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
                return _url + " " + Database;
            }
        }

        public bool CanBeFailover() =>
            IgnoredClient == false && Disabled == false && (SpecifiedCollections == null || SpecifiedCollections.Count == 0);

        public override string ToString() => $"{nameof(Url)}: {Url}, {nameof(Database)}: {Database}";

        public bool Equals(ReplicationDestination other) => IsEqualTo(other);

        public bool IsMatch(ReplicationDestination other)
        {
            return
                string.Equals(Url, other.Url, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase);
        }

        public bool IsEqualTo(ReplicationDestination other)
        {
            return string.Equals(Username, other.Username) && string.Equals(Password, other.Password) &&
                   string.Equals(Domain, other.Domain)&&
                   string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase) &&
                   TransitiveReplicationBehavior == other.TransitiveReplicationBehavior &&
                   IgnoredClient.Equals(other.IgnoredClient) && Disabled.Equals(other.Disabled) &&
                   ((string.Equals(Url, other.Url, StringComparison.OrdinalIgnoreCase) && string.IsNullOrWhiteSpace(ClientVisibleUrl)) ||
                   (!string.IsNullOrWhiteSpace(ClientVisibleUrl) && string.Equals(ClientVisibleUrl, other.ClientVisibleUrl, StringComparison.OrdinalIgnoreCase))) &&
                   DictionaryExtensions.ContentEquals(SpecifiedCollections, other.SpecifiedCollections);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ReplicationDestination)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = Username?.GetHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Password?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Domain?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Database?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (int)TransitiveReplicationBehavior;
                hashCode = (hashCode * 397) ^ IgnoredClient.GetHashCode();
                hashCode = (hashCode * 397) ^ Disabled.GetHashCode();
                hashCode = (hashCode * 397) ^ (ClientVisibleUrl?.GetHashCode() ?? 0);
                return hashCode;
            }
        }

        public DynamicJsonValue ToJson()
        {
            var json = new DynamicJsonValue
            {
                [nameof(AuthenticationScheme)] = AuthenticationScheme,
                [nameof(ClientVisibleUrl)] = ClientVisibleUrl,
                [nameof(Database)] = Database,
                [nameof(Disabled)] = Disabled,
                [nameof(Domain)] = Domain,
                [nameof(Humane)] = Humane,
                [nameof(IgnoredClient)] = IgnoredClient,
                [nameof(Password)] = Password,
                [nameof(SkipIndexReplication)] = SkipIndexReplication,
                [nameof(TransitiveReplicationBehavior)] = TransitiveReplicationBehavior,
                [nameof(Url)] = Url,
                [nameof(Username)] = Username
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

    /// <summary>
    /// Options for how to replicate replicated documents
    /// </summary>
    public enum TransitiveReplicationOptions
    {
        /// <summary>
        /// Don't replicate replicated documents
        /// </summary>
        None,
        /// <summary>
        /// Replicate replicated documents
        /// </summary>
        Replicate
    }
}