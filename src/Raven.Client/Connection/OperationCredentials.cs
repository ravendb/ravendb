// -----------------------------------------------------------------------
//  <copyright file="OperationCredentials.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Raven.Client.Connection
{
    public class OperationCredentials
    {
        public OperationCredentials(string apiKey)
        {
            ApiKey = apiKey;
        }

        public string ApiKey { get; private set; }



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
                return (ApiKey != null ? ApiKey.GetHashCode() : 0);
            }
        }
    }
}