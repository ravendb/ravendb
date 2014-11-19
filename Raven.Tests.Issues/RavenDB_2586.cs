// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2586.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Client.Extensions;
using Raven.Smuggler;
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
                var smugglerApi = new SmugglerDatabaseApi();

                var options = new SmugglerBetweenOptions<RavenConnectionStringOptions>
				              {
									From = new RavenConnectionStringOptions
					                {
						                Url = store.Url, 
										DefaultDatabase = "DB1"
					                },
									To = new RavenConnectionStringOptions
					                {
										Url = store.Url, 
										DefaultDatabase = "DB2"
					                }
				              };

				var aggregateException = Assert.Throws<AggregateException>(() => smugglerApi.Between(options).Wait());
				var exception = aggregateException.ExtractSingleInnerException();
				Assert.True(exception.Message.StartsWith("Smuggler does not support database creation (database 'DB1' on server"));

				store.DatabaseCommands.GlobalAdmin.EnsureDatabaseExists("DB1");

				aggregateException = Assert.Throws<AggregateException>(() => smugglerApi.Between(options).Wait());
				exception = aggregateException.ExtractSingleInnerException();
				Assert.True(exception.Message.StartsWith("Smuggler does not support database creation (database 'DB2' on server"));
			}
		}
	}
}