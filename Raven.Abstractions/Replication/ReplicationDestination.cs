//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
		public string ConnectionStringName { get; set; }

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
		/// Gets or sets if the replication will use ConnectionString or Url
		/// </summary>
		public bool UseConnectionString { get; set; }

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
