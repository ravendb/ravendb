using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.ETL.Providers.Raven
{
    public class RavenDestination : EtlDestination
    {
        private string _url;

        public string Url
        {
            get => _url;
            set => _url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
        }

        public string Database { get; set; }

        public string ApiKey { get; set; }

        public int? LoadRequestTimeoutInSec { get; set; }

        public override bool Validate(ref List<string> errors)
        {
            if (string.IsNullOrEmpty(Database))
                errors.Add($"{nameof(Database)} cannot be empty");

            if (string.IsNullOrEmpty(Url))
                errors.Add($"{nameof(Url)} cannot be empty");
            
            return errors.Count == 0;
        }

        public override string ToString()
        {
            return $"{Database}@{Url}";
        }

        protected bool Equals(RavenDestination other)
        {
            return string.Equals(Url, other.Url) && string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((RavenDestination)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = 1;
                hashCode = (hashCode * 397) ^ (Url != null ? Url.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Database != null ? Database.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}