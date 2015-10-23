// -----------------------------------------------------------------------
//  <copyright file="FileToFileTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Impl.Files;
using Raven.Smuggler.Database.Impl.Remote;

using Xunit;

namespace Raven.Tests.Smuggler
{
	public class FileToFileTests : SmugglerTest
	{
		[Fact]
		public void Northwind_RemoteToFile_FileToRemote_Test()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				DeployNorthwind(store1);
				WaitForIndexing(store1);

				var outputFile = Path.Combine(NewDataPath(forceCreateDir: true), "backup.ravendump");

				var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerRemoteSource(store1), new DatabaseSmugglerFileDestination(outputFile));
				smuggler.Execute();

				smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerFileSource(outputFile), new DatabaseSmugglerRemoteDestination(store2));
				smuggler.Execute();

				WaitForIndexing(store2);

				var statistics = store2.DatabaseCommands.GetStatistics();

				Assert.Equal(1059, statistics.CountOfDocuments);
				Assert.Equal(4, statistics.CountOfIndexes);
				Assert.Equal(1, statistics.CountOfResultTransformers);
			}
		}
	}
}