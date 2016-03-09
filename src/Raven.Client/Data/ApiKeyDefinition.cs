using System.Collections.Generic;
using System.Linq;
using  Raven.Imports.Newtonsoft.Json;

namespace Raven.Abstractions.Data
{
    //    public class ApiKeyDefinition
    //    {
    //        /// <summary>
    //        /// Document identifier.
    //        /// </summary>
    //        public string Id { get; set; }

    //        /// <summary>
    //        /// API key name.
    //        /// </summary>
    //        public string Name { get; set; }

    //        /// <summary>
    //        /// API key secret.
    //        /// </summary>
    //        public string Secret { get; set; }

    //        /// <summary>
    //        /// Full API key in following format: '{Name}/{Secret}'.
    //        /// </summary>
    //        [JsonIgnore]
    //        public string FullApiKey
    //        {
    //            get
    //            {
    //                if (string.IsNullOrWhiteSpace(Name) || string.IsNullOrWhiteSpace(Secret))
    //                    return "Must set both name and secret to get the full api key";

    //                return Name + "/" + Secret;
    //            }
    //        }

    //        /// <summary>
    //        /// Connection string for API Key in following format: 'ApiKey = {FullApiKey}; Database = {DbName}'.
    //        /// </summary>
    //        [JsonIgnore]
    //        public string ConnectionString
    //        {
    //            get
    //            {
    //                if (string.IsNullOrWhiteSpace(Name) || string.IsNullOrWhiteSpace(Secret))
    //                    return null;

    //                return string.Format(@"ApiKey = {0}; Database = {1}", FullApiKey, DbName);
    //            }
    //        }

    //        /// <summary>
    //        /// Returns Id of a first database. Null if there are no databases defined for this key.
    //        /// </summary>
    //        [JsonIgnore]
    //        private string DbName
    //        {
    //            get
    //            {
    //                var access = Databases.FirstOrDefault();
    //                return access == null ? "DbName" : access.TenantId;
    //            }
    //        }

    //        /// <summary>
    //        /// Indicates if API key is enabled or not.
    //        /// </summary>
    //        public bool Enabled { get; set; }

    //        /// <summary>
    //        /// List of databases (with detailed permissions) for which this API key works.
    //        /// </summary>
    //        public List<ResourceAccess> Databases { get; set; }

    //        public ApiKeyDefinition()
    //        {
    //            Databases = new List<ResourceAccess>();
    //        }

    //        protected bool Equals(ApiKeyDefinition other)
    //        {
    //            var baseEqual = string.Equals(Id, other.Id) && Enabled.Equals(other.Enabled) && Equals(Databases.Count, other.Databases.Count) &&
    //                   string.Equals(Secret, other.Secret) && string.Equals(Name, other.Name);

    //            if (baseEqual == false)
    //                return false;

    //            for (var i = 0; i < Databases.Count; i++)
    //            {
    //                if (Databases[i].Equals(other.Databases[i]) == false)
    //                    return false;
    //            }

    //            return true;
    //        }

    //        public override bool Equals(object obj)
    //        {
    //            if (ReferenceEquals(null, obj)) return false;
    //            if (ReferenceEquals(this, obj)) return true;
    //            if (obj.GetType() != GetType()) return false;
    //            return Equals((ApiKeyDefinition)obj);
    //        }

    //        public override int GetHashCode()
    //        {
    //            unchecked
    //            {
    //                var hashCode = (Id != null ? Id.GetHashCode() : 0);
    //                hashCode = (hashCode * 397) ^ Enabled.GetHashCode();
    //                hashCode = (hashCode * 397) ^ (Databases != null ? Databases.GetHashCode() : 0);
    //                hashCode = (hashCode * 397) ^ (Secret != null ? Secret.GetHashCode() : 0);
    //                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
    //                return hashCode;
    //            }
    //        }
    //    }

    //    public class ResourceAccess
    //    {
    //        /// <summary>
    //        /// Indicates if administrative acesss should be granted.
    //        /// </summary>
    //        public bool Admin { get; set; }

    //        /// <summary>
    //        /// Id a database.
    //        /// </summary>
    //        public string TenantId { get; set; }

    //        /// <summary>
    //        /// Indicates if read-only acesss should be granted.
    //        /// </summary>
    //        public bool ReadOnly { get; set; }

    //        protected bool Equals(ResourceAccess other)
    //        {
    //            return Admin.Equals(other.Admin) && string.Equals(TenantId, other.TenantId) && ReadOnly.Equals(other.ReadOnly);
    //        }

    //        public override bool Equals(object obj)
    //        {
    //            if (ReferenceEquals(null, obj)) return false;
    //            if (ReferenceEquals(this, obj)) return true;
    //            if (obj.GetType() != GetType()) return false;
    //            return Equals((ResourceAccess)obj);
    //        }

    //        public override int GetHashCode()
    //        {
    //            unchecked
    //            {
    //                var hashCode = Admin.GetHashCode();
    //                hashCode = (hashCode * 397) ^ (TenantId != null ? TenantId.GetHashCode() : 0);
    //                hashCode = (hashCode * 397) ^ ReadOnly.GetHashCode();
    //                return hashCode;
    //            }
    //        }
    //    }

    public class ResourceAccess
    {        
        /// <summary>
        /// Id a database.
        /// </summary>
        public string TenantId { get; set; }

        /// <summary>
        /// Indicates if read-only / Admin acesss should be granted.
        /// </summary>
        public string AccessMode { get; set; }
    }





}
