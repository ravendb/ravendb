//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Sparrow;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Replication
{
    /// <summary>
    /// Data class for replication destination
    /// </summary>
    public abstract class ReplicationNode : IEquatable<ReplicationNode>
    {
        /// <summary>
        /// The name of the connection string specified in the 
        /// server configuration file. 
        /// Override all other properties of the destination
        /// </summary>

        private string _url;

        /// <summary>
        /// This is a protection field that indicates if an hashset was requested already so we won't modify fields that are been used to calculate it afterwards.
        /// If you ovverride GetHashCode you should make sure to set this field to true at the end of the method.
        /// </summary>
        protected bool HashCodeSealed;

        /// <summary>
        /// Gets or sets the URL of the replication destination
        /// </summary>
        /// <value>The URL.</value>
        public string Url
        {
            get => _url;
            set => _url = value?.TrimEnd('/');
        }

        /// <summary>
        /// The database to use
        /// </summary>
        public string Database;

        /// <summary>
        /// Used to indicate whether external replication is disabled.
        /// </summary>
        public bool Disabled { get; set; }

        public bool Equals(ReplicationNode other) => IsEqualTo(other);

        public virtual bool IsEqualTo(ReplicationNode other)
        {
            return string.Equals(Database, other.Database, StringComparison.OrdinalIgnoreCase);
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
            throw new InvalidOperationException(
                "Derived classes of 'ReplicationNode' must override 'GetHashCode' and set 'HashCodeSealed' at the end, if you see this error it is likley a bug.");
        }

        protected static ulong CalculateStringHash(string s)
        {
            return string.IsNullOrEmpty(s) ? 0 : Hashing.XXHash64.Calculate(s, Encodings.Utf8);
        }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(Database)] = Database,
                [nameof(Url)] = Url,
                [nameof(Disabled)] = Disabled
            };
        }

        public override string ToString()
        {
            var str = $"{FromString()}";
            if (Disabled)
                str += " - DISABLED";
            return str;
        }

        public abstract string FromString();
    }

   
}
