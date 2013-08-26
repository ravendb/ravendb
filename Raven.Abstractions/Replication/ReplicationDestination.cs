//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
#if NETFX_CORE
using Raven.Client.WinRT.MissingFromWinRT;
#else
using System.ComponentModel;
#endif

namespace Raven.Abstractions.Replication
{
	/// <summary>
	/// Data class for replication destination documents
	/// </summary>
	public class ReplicationDestination
	{
		/// <summary>
		/// The name of the connection string specified in the 
		/// server configuration file. 
		/// Override all other properties of the destination
		/// </summary>

		private string url;

		/// <summary>
		/// Gets or sets the URL of the replication destination
		/// </summary>
		/// <value>The URL.</value>
		public string Url
		{
			get { return url; }
			set 
			{
				url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value;
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
		/// The replication server api key to use
		/// </summary>
		public string ApiKey { get; set; }

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

		protected bool Equals(ReplicationDestination other)
		{
			return string.Equals(Username, other.Username) && string.Equals(Password, other.Password) &&
			       string.Equals(Domain, other.Domain) && string.Equals(ApiKey, other.ApiKey) &&
			       string.Equals(Database, other.Database) &&
			       TransitiveReplicationBehavior == other.TransitiveReplicationBehavior &&
			       IgnoredClient.Equals(other.IgnoredClient) && Disabled.Equals(other.Disabled) &&
			       string.Equals(ClientVisibleUrl, other.ClientVisibleUrl);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((ReplicationDestination) obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				var hashCode = (Username != null ? Username.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (Password != null ? Password.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (Domain != null ? Domain.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (ApiKey != null ? ApiKey.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (Database != null ? Database.GetHashCode() : 0);
				hashCode = (hashCode*397) ^ (int) TransitiveReplicationBehavior;
				hashCode = (hashCode*397) ^ IgnoredClient.GetHashCode();
				hashCode = (hashCode*397) ^ Disabled.GetHashCode();
				hashCode = (hashCode*397) ^ (ClientVisibleUrl != null ? ClientVisibleUrl.GetHashCode() : 0);
				return hashCode;
			}
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
		[Description("Changed only")]
		None,
		/// <summary>
		/// Replicate replicated documents
		/// </summary>
		[Description("Changed and replicated")]
		Replicate
	}
}