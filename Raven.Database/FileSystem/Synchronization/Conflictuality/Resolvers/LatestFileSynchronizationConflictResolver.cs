// -----------------------------------------------------------------------
//  <copyright file="LatestFileSynchronizationConflictResolver.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality.Resolvers
{
	[PartNotDiscoverable]
	public class LatestFileSynchronizationConflictResolver : AbstractFileSynchronizationConflictResolver
	{
		public static LatestFileSynchronizationConflictResolver Instance = new LatestFileSynchronizationConflictResolver();

		public override bool TryResolve(string fileName, RavenJObject localMedatada, RavenJObject remoteMetadata, out ConflictResolutionStrategy resolutionStrategy)
		{
			var remoteFileLastModified = GetLastModified(remoteMetadata);
			var localFileLastModified = GetLastModified(localMedatada);

			if (remoteFileLastModified > localFileLastModified)
				resolutionStrategy = ConflictResolutionStrategy.RemoteVersion;
			else
				resolutionStrategy = ConflictResolutionStrategy.CurrentVersion;

			return true;
		}

		private static DateTimeOffset GetLastModified(RavenJObject metadata)
		{
			if (metadata.ContainsKey(Constants.LastModified))
				return metadata.Value<DateTimeOffset>(Constants.LastModified);

			if (metadata.ContainsKey(Constants.RavenLastModified))
				return metadata.Value<DateTimeOffset>(Constants.RavenLastModified);

			throw new InvalidOperationException("Could not find last modification date in metadata");
		}
	}
}