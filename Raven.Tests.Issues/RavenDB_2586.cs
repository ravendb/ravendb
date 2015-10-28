// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2586.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2586 : RavenTest
	{
		[Fact]
		public void SmugglerBetweenOperationShouldNotCreateDatabases()
		{
			using (var store = NewRemoteDocumentStore())
			{
                var smuggler = new DatabaseSmuggler(
                    new DatabaseSmugglerOptions(),
                    new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url,
                        Database = "DB1"
                    }), new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                    {
                        Url = store.Url,
                        Database = "DB2"
                    }));

				var exception = Assert.Throws<SmugglerException>(() => smuggler.Execute());
				Assert.True(exception.Message.StartsWith("Smuggler does not support database creation (database 'DB1' on server"));

				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("DB1");

                exception = Assert.Throws<SmugglerException>(() => smuggler.Execute());
				Assert.True(exception.Message.StartsWith("Smuggler does not support database creation (database 'DB2' on server"));
			}
		}
	}
}