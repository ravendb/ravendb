// -----------------------------------------------------------------------
//  <copyright file="SynchronizationConfig.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Replication;

namespace Raven.Abstractions.FileSystem
{
	public class SynchronizationConfig
	{
		public SynchronizationConfig()
		{
			FileConflictResolution = StraightforwardConflictResolution.None;
			MaxNumberOfSynchronizationsPerDestination = 5;
			SynchronizationLockTimeout = TimeSpan.FromMinutes(10);
		}

		public StraightforwardConflictResolution FileConflictResolution { get; set; } 

		public int MaxNumberOfSynchronizationsPerDestination { get; set; }

		public TimeSpan SynchronizationLockTimeout { get; set; }
	}
}