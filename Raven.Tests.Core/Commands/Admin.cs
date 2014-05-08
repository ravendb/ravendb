// -----------------------------------------------------------------------
//  <copyright file="Admin.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Core.Commands
{
	public class Admin : RavenCoreTestBase
	{
		[Fact]
		public void CanManageIndexingProcess()
		{
			using (var store = GetDocumentStore())
			{
				var adminCommands = store.DatabaseCommands.Admin;

				var indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Indexing", indexingStatus);

				adminCommands.StopIndexing();

				indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Paused", indexingStatus);

				adminCommands.StartIndexing();

				indexingStatus = adminCommands.GetIndexingStatus();

				Assert.Equal("Indexing", indexingStatus);
			}
		}
	}
}