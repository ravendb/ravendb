// -----------------------------------------------------------------------
//  <copyright file="AbstractFileSynchronizationConflictResolver.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Synchronization.Conflictuality.Resolvers
{
    [InheritedExport]
    public abstract class AbstractFileSynchronizationConflictResolver
    {
        public abstract bool TryResolve(string fileName, RavenJObject localMedatada, RavenJObject remoteMetadata, out ConflictResolutionStrategy resolutionStrategy); 
    }
}
