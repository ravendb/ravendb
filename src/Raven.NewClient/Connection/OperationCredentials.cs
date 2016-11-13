// -----------------------------------------------------------------------
//  <copyright file="OperationCredentials.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Net;

namespace Raven.Abstractions.Connection
{
    public class OperationCredentials
    {
        public OperationCredentials(string apiKey, ICredentials credentials)
        {
            ApiKey = apiKey;
            Credentials = credentials;
        }

        public ICredentials Credentials { get; private set; }

        public string ApiKey { get; private set; }

        public bool HasCredentials()
        {
            return !string.IsNullOrEmpty(ApiKey) || Credentials != null;
        }

        protected bool Equals(OperationCredentials other)
        {
            return Equals(Credentials, other.Credentials) && string.Equals(ApiKey, other.ApiKey);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }
            if (ReferenceEquals(this, obj))
            {
                return true;
            }
            if (obj.GetType() != GetType())
            {
                return false;
            }
            return Equals((OperationCredentials)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Credentials != null ? Credentials.GetHashCode() : 0) * 397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
            }
        }
    }
}