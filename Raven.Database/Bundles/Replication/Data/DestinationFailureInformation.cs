//-----------------------------------------------------------------------
// <copyright file="DestinationFailureInformation.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Bundles.Replication.Data
{
	public class DestinationFailureInformation
	{
		public string Destination { get; set; }
		public int FailureCount { get; set; }
	}
}
