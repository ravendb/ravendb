// -----------------------------------------------------------------------
//  <copyright file="RemoteSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Database.Smuggler.Embedded;
using Raven.Smuggler.Database;
using Raven.Tests.Smuggler.Helpers;

using Xunit;

namespace Raven.Tests.Smuggler.OperateOnTypes
{
	public class EmbeddedSource : SmugglerTest
	{
		[Fact]
		public void ShouldSmuggleNothing()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var database = servers[0].Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;

				DeployNorthwind(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
				{
					OperateOnTypes = DatabaseItemType.RemoveAnalyzers
				}, new DatabaseSmugglerEmbeddedSource(database), destination);
				smuggler.Execute();

				Assert.Equal(0, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(0, destination.WroteIdentities);
				Assert.Equal(0, destination.WroteIndexes);
				Assert.Equal(0, destination.WroteTransformers);
			}
		}

		[Fact]
		public void ShouldSmuggleOnlyDocuments()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var database = servers[0].Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;

				DeployNorthwind(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
				{
					OperateOnTypes = DatabaseItemType.Documents
				}, new DatabaseSmugglerEmbeddedSource(database), destination);
				smuggler.Execute();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(0, destination.WroteIndexes);
				Assert.Equal(0, destination.WroteTransformers);
			}
		}

		[Fact]
		public void ShouldSmuggleOnlyIndexes()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var database = servers[0].Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;

				DeployNorthwind(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
				{
					OperateOnTypes = DatabaseItemType.Indexes
				}, new DatabaseSmugglerEmbeddedSource(database), destination);
				smuggler.Execute();

				Assert.Equal(0, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(0, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(0, destination.WroteTransformers);
			}
		}

		[Fact]
		public void ShouldSmuggleOnlyTransformers()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var database = servers[0].Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;

				DeployNorthwind(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
				{
					OperateOnTypes = DatabaseItemType.Transformers
				}, new DatabaseSmugglerEmbeddedSource(database), destination);
				smuggler.Execute();

				Assert.Equal(0, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(0, destination.WroteIdentities);
				Assert.Equal(0, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}

		[Fact]
		public void ShouldSmuggleEverything()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var database = servers[0].Options.DatabaseLandlord.GetResourceInternal(store.DefaultDatabase).Result;

				DeployNorthwind(store);

				var destination = new DatabaseSmugglerCountingDestination();
				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
				{
					OperateOnTypes = DatabaseItemType.Documents | DatabaseItemType.Indexes | DatabaseItemType.Transformers
				}, new DatabaseSmugglerEmbeddedSource(database), destination);
				smuggler.Execute();

				Assert.Equal(1059, destination.WroteDocuments);
				Assert.Equal(0, destination.WroteDocumentDeletions);
				Assert.Equal(1, destination.WroteIdentities);
				Assert.Equal(4, destination.WroteIndexes);
				Assert.Equal(1, destination.WroteTransformers);
			}
		}
	}
}