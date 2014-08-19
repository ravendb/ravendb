// -----------------------------------------------------------------------
//  <copyright file="RemoveFileSynchronizationConflictResolver.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Synchronization.Conflictuality.Resolvers
{
	[PartNotDiscoverable]
	public class RemoveFileSynchronizationConflictResolver : AbstractFileSynchronizationConflictResolver
	{
		public static RemoveFileSynchronizationConflictResolver Instance = new RemoveFileSynchronizationConflictResolver();

		public override bool TryResolve(string fileName, RavenJObject localMedatada, RavenJObject remoteMetadata, out ConflictResolutionStrategy resolutionStrategy)
		{
			resolutionStrategy = ConflictResolutionStrategy.RemoteVersion;
			return true;
		}
	}
}