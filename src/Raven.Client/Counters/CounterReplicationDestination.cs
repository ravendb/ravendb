// -----------------------------------------------------------------------
//  <copyright file="CounterStorageReplicationDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;
using System.Net;
using  Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Counters
{
    public class CounterReplicationDestination
    {
        public string ServerUrl { get; set; }

        public string CounterStorageName { get; set; }

        public string CounterStorageUrl
        {
            get
            {
                Debug.Assert(String.IsNullOrWhiteSpace(CounterStorageName) == false);
                Debug.Assert(String.IsNullOrWhiteSpace(ServerUrl) == false);

                return string.Format("{0}cs/{1}", ServerUrl, CounterStorageName);
            }
        }

        public string Username { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }

        public string ApiKey { get; set; }

        public bool Disabled { get; set; }

        [JsonIgnore]
        public ICredentials Credentials
        {
            get
            {
                if (string.IsNullOrEmpty(Username) == false)
                {
                    return string.IsNullOrEmpty(Domain)
                                      ? new NetworkCredential(Username, Password)
                                      : new NetworkCredential(Username, Password, Domain);
                }
                return null;
            }
        }

        protected bool Equals(CounterReplicationDestination other)
        {
            return string.Equals(ServerUrl, other.ServerUrl) && string.Equals(ApiKey, other.ApiKey) && string.Equals(Domain, other.Domain) &&
                string.Equals(Password, other.Password) && string.Equals(Username, other.Username) && string.Equals(CounterStorageName, other.CounterStorageName);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((CounterReplicationDestination)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ServerUrl != null ? ServerUrl.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Domain != null ? Domain.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Password != null ? Password.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Username != null ? Username.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (CounterStorageName != null ? CounterStorageName.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
