// -----------------------------------------------------------------------
//  <copyright file="DatabaseRecord.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.ClusterManager.Models
{
	public class DatabaseRecord
	{
		public string Name { get; set; }
		public string ServerId { get; set; }
		public string ServerUrl { get; set; }
		public bool IsReplicationEnabled { get; set; }
	}
}