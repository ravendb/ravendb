// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3759.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler;
using Raven.Smuggler;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Impl;
using Raven.Smuggler.Database.Impl.Streams;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3759 : RavenTest
	{
		[Fact]
		public async Task T1()
		{
			using (var store = NewRemoteDocumentStore())
			{
				DeployNorthwind(store);

				WaitForIndexing(store);

				using (var input = new MemoryStream())
				using (var output = new MemoryStream())
				{
					var oldSmuggler = new SmugglerDatabaseApi();
					await oldSmuggler
						.ExportData(new SmugglerExportOptions<RavenConnectionStringOptions>
						{
							From = new RavenConnectionStringOptions
							{
								DefaultDatabase = store.DefaultDatabase,
								Url = store.Url
							},
							ToStream = input
						});

					var smuggler = new DatabaseSmuggler(new DatabaseSmugglerOptions(), new DatabaseSmugglerStreamSource(input, CancellationToken.None), new DatabaseSmugglerStreamDestination(output));
					await smuggler.ExecuteAsync();

				}
			}
		}
	}
}