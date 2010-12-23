//-----------------------------------------------------------------------
// <copyright file="ReplicationDestination.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Replication.Data
{
	/// <summary>
	/// Data class for replication destination documents
	/// </summary>
    public class ReplicationDestination
    {
		/// <summary>
		/// Gets or sets the URL of the replication destination
		/// </summary>
		/// <value>The URL.</value>
        public string Url { get; set; }
    }
}
