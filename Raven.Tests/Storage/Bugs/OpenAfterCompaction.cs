//-----------------------------------------------------------------------
// <copyright file="OpenAfterCompaction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions;
using Raven.Json.Linq;
using Raven.Munin;
using Raven.Storage.Managed.Impl;
using Xunit;

namespace Raven.Tests.Storage.Bugs
{
	public class OpenAfterCompaction 
	{
		[Fact]
		public void CanOpenAfterCompaction()
		{
			var memoryPersistentSource = new MemoryPersistentSource();
			var tableStorage = new TableStorage(memoryPersistentSource);
			tableStorage.Initialze();
			tableStorage.BeginTransaction();
			tableStorage.Documents.Put(new RavenJObject
			{
				{"key", "1"},
				{"etag", Guid.NewGuid().ToByteArray()},
				{"modified", SystemTime.UtcNow},
				{"id", 1},
				{"entityName", "test"}
			}, new byte[512] );

			tableStorage.Documents.Put(new RavenJObject
			{
				{"key", "2"},
				{"etag", Guid.NewGuid().ToByteArray()},
				{"modified", SystemTime.UtcNow},
				{"id", 1},
				{"entityName", "test"}
			}, new byte[512] );
			tableStorage.Commit();

			tableStorage.BeginTransaction();
			tableStorage.Documents.Remove(new RavenJObject { { "key", "1" } });
			tableStorage.Commit();

			tableStorage.Compact();


			var remoteManagedStorageState = memoryPersistentSource.CreateRemoteAppDomainState();
			new TableStorage(new MemoryPersistentSource(remoteManagedStorageState.Log)).Initialze();
		}
	}
}
