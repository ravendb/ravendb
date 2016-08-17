// -----------------------------------------------------------------------
//  <copyright file="NodeConnection.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using Newtonsoft.Json;
using Raven.Abstractions.Connection;

namespace Rachis.Transport
{
    public class NodeConnectionInfo
    {
        private Uri uri;
        public Uri Uri
        {
            get { return uri; }
            set
            {
                uri = value;
                absoluteUri = uri.AbsoluteUri[uri.AbsoluteUri.Length - 1] == '/' ? uri.AbsoluteUri : uri.AbsoluteUri + '/';
            }
        }
        [JsonIgnore]
        private string absoluteUri;
        /// <summary>
        /// Returns the absoluteUri of the node, making sure that under iis the uri ends with '/'
        /// </summary>
        [JsonIgnore]
        public string AbsoluteUri => absoluteUri;

        public string Name { get; set; }

        public string Username { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }

        public string ApiKey { get; set; }

        public bool IsNoneVoter { get; set; }

        public override string ToString()
        {
            return Name;
        }

        protected bool Equals(NodeConnectionInfo other)
        {
            return Equals(Uri, other.Uri) && string.Equals(Name, other.Name) && string.Equals(Username, other.Username) && string.Equals(Password, other.Password) && string.Equals(Domain, other.Domain) && string.Equals(ApiKey, other.ApiKey);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((NodeConnectionInfo) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Uri != null ? Uri.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (Username != null ? Username.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (Password != null ? Password.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (Domain != null ? Domain.GetHashCode() : 0);
                hashCode = (hashCode*397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static bool operator ==(NodeConnectionInfo left, NodeConnectionInfo right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(NodeConnectionInfo left, NodeConnectionInfo right)
        {
            return !Equals(left, right);
        }

        public OperationCredentials ToOperationCredentials()
        {
            if (Username != null)
            {
                var networkCredentials = new NetworkCredential(Username, Password, Domain ?? string.Empty);
                return new OperationCredentials(ApiKey, networkCredentials);
            }

            return new OperationCredentials(ApiKey, null);
        }

        public bool HasCredentials()
        {
            return !string.IsNullOrEmpty(ApiKey) || Username != null;
        }
    }
}
