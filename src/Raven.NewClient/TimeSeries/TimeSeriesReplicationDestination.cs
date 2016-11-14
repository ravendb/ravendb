// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesReplicationDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;

namespace Raven.NewClient.Abstractions.TimeSeries
{
    public class TimeSeriesReplicationDestination
    {
        public string ServerUrl { get; set; }

        public string TimeSeriesName { get; set; }

        public string TimeSeriesUrl
        {
            get
            {
                Debug.Assert(String.IsNullOrWhiteSpace(TimeSeriesName) == false);
                Debug.Assert(String.IsNullOrWhiteSpace(ServerUrl) == false);

                return string.Format("{0}ts/{1}", ServerUrl, TimeSeriesName);
            }
        }

        public string Username { get; set; }

        public string Password { get; set; }

        public string Domain { get; set; }

        public string ApiKey { get; set; }

        public bool Disabled { get; set; }

        protected bool Equals(TimeSeriesReplicationDestination other)
        {
            return string.Equals(ServerUrl, other.ServerUrl) && string.Equals(ApiKey, other.ApiKey) && string.Equals(Domain, other.Domain) &&
                string.Equals(Password, other.Password) && string.Equals(Username, other.Username) && string.Equals(TimeSeriesName, other.TimeSeriesName);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((TimeSeriesReplicationDestination)obj);
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
                hashCode = (hashCode * 397) ^ (TimeSeriesName != null ? TimeSeriesName.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
