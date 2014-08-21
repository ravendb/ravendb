// -----------------------------------------------------------------------
//  <copyright file="DatabaseConfiguration.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class DatabaseConfiguration : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (var store = NewDocumentStore())
			{
				var configuration = store.DatabaseCommands.Admin.GetDatabaseConfiguration();
				Assert.NotNull(configuration);

				configuration = store.DatabaseCommands.ForSystemDatabase().Admin.GetDatabaseConfiguration();
				Assert.NotNull(configuration);
			}
		}
	}
}