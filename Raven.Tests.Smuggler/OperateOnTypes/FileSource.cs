// -----------------------------------------------------------------------
//  <copyright file="FileSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Abstractions.Database.Smuggler.Data;
using Raven.Abstractions.Smuggler;
using Raven.Client.Document;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Impl.Files;
using Raven.Smuggler.Database.Impl.Remote;
using Raven.Tests.Smuggler.Helpers;

using Xunit;

namespace Raven.Tests.Smuggler.OperateOnTypes
{
	public class FileSource : SmugglerTest
	{
		private readonly DocumentStore _store;

		private readonly string _path;

		public FileSource()
		{
			_store = NewRemoteDocumentStore();
			_path = Path.Combine(NewDataPath(forceCreateDir: true), "backup.ravendump");
			DeployNorthwindAndExportToFile(_store, _path);
		}

		[Fact]
		public void ShouldSmuggleNothing()
		{
			var destination = new DatabaseSmugglerCountingDestination();
			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
			{
				OperateOnTypes = ItemType.RemoveAnalyzers
			}, new DatabaseSmugglerFileSource(_path), destination);
			smuggler.Execute();

			Assert.Equal(0, destination.WroteDocuments);
			Assert.Equal(0, destination.WroteDocumentDeletions);
			Assert.Equal(0, destination.WroteIdentities);
			Assert.Equal(0, destination.WroteIndexes);
			Assert.Equal(0, destination.WroteTransformers);
		}

		[Fact]
		public void ShouldSmuggleOnlyDocuments()
		{
			var destination = new DatabaseSmugglerCountingDestination();
			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
			{
				OperateOnTypes = ItemType.Documents
			}, new DatabaseSmugglerFileSource(_path), destination);
			smuggler.Execute();

			Assert.Equal(1059, destination.WroteDocuments);
			Assert.Equal(0, destination.WroteDocumentDeletions);
			Assert.Equal(1, destination.WroteIdentities);
			Assert.Equal(0, destination.WroteIndexes);
			Assert.Equal(0, destination.WroteTransformers);
		}

		[Fact]
		public void ShouldSmuggleOnlyIndexes()
		{
			var destination = new DatabaseSmugglerCountingDestination();
			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
			{
				OperateOnTypes = ItemType.Indexes
			}, new DatabaseSmugglerFileSource(_path), destination);
			smuggler.Execute();

			Assert.Equal(0, destination.WroteDocuments);
			Assert.Equal(0, destination.WroteDocumentDeletions);
			Assert.Equal(0, destination.WroteIdentities);
			Assert.Equal(4, destination.WroteIndexes);
			Assert.Equal(0, destination.WroteTransformers);
		}

		[Fact]
		public void ShouldSmuggleOnlyTransformers()
		{
			var destination = new DatabaseSmugglerCountingDestination();
			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
			{
				OperateOnTypes = ItemType.Transformers
			}, new DatabaseSmugglerFileSource(_path), destination);
			smuggler.Execute();

			Assert.Equal(0, destination.WroteDocuments);
			Assert.Equal(0, destination.WroteDocumentDeletions);
			Assert.Equal(0, destination.WroteIdentities);
			Assert.Equal(0, destination.WroteIndexes);
			Assert.Equal(1, destination.WroteTransformers);
		}

		[Fact]
		public void ShouldSmuggleEverything()
		{
			var destination = new DatabaseSmugglerCountingDestination();
			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions
			{
				OperateOnTypes = ItemType.Documents | ItemType.Indexes | ItemType.Transformers
			}, new DatabaseSmugglerFileSource(_path), destination);
			smuggler.Execute();

			Assert.Equal(1059, destination.WroteDocuments);
			Assert.Equal(0, destination.WroteDocumentDeletions);
			Assert.Equal(1, destination.WroteIdentities);
			Assert.Equal(4, destination.WroteIndexes);
			Assert.Equal(1, destination.WroteTransformers);
		}

		private static void DeployNorthwindAndExportToFile(DocumentStore store, string path)
		{
			DeployNorthwind(store);

			var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(store), new DatabaseSmugglerFileDestination(path));
			smuggler.Execute();
		}

		public override void Dispose()
		{
			_store?.Dispose();

			base.Dispose();
		}
	}
}