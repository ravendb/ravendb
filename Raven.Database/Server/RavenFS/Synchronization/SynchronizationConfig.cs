// -----------------------------------------------------------------------
//  <copyright file="SynchronizationConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Replication;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public class SynchronizationConfig
	{
		public StraightforwardConflictResolution FileConflictResolution { get; set; } 
	}
}