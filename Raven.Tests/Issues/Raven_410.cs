// -----------------------------------------------------------------------
//  <copyright file="Raven_401.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Xunit;
using Raven.Client.Extensions;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Issues
{
	public class Raven_410 : RavenTest
	{
		[Fact]
		public void CanCreateDatabaseUsingApi()
		{
			using(GetNewServer())
			using(var store = new DocumentStore
			                  	{
			                  		Url = "http://localhost:8079"
			                  	}.Initialize())
			{
				store.DatabaseCommands.CreateDatabase(new DatabaseDocument
				                                      	{
															Id = "mydb",
				                                      		Settings =
				                                      			{
				                                      				{"Raven/DataDir", @"~\Databases\Mine"}
				                                      			}
				                                      	});

				Assert.DoesNotThrow(() => store.DatabaseCommands.ForDatabase("mydb").Get("test"));
			}
		}

		[Fact]
		public void CanCreateDatabaseWithHiddenData()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				store.DatabaseCommands.CreateDatabase(new DatabaseDocument
				                                      	{
				                                      		Id = "mydb",
				                                      		Settings =
				                                      			{
				                                      				{"Raven/DataDir", @"~\Databases\Mine"}
				                                      			},
															SecuredSettings =
																{
																	{"Secret", "Pass"}
																}
				                                      	});

				var jsonDocument = store.DatabaseCommands.Get("Raven/Databases/mydb");
				var jsonDeserialization = jsonDocument.DataAsJson.JsonDeserialization<DatabaseDocument>();
				Assert.NotEqual("Pass", jsonDeserialization.SecuredSettings["Secret"]);
			}
		}

		[Fact]
		public void TheDatabaseCanReadSecretInfo()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				store.DatabaseCommands.CreateDatabase(new DatabaseDocument
				{
					Id = "mydb",
					Settings =
				                                      			{
				                                      				{"Raven/DataDir", @"~\Databases\Mine"}
				                                      			},
					SecuredSettings =
																{
																	{"Secret", "Pass"}
																}
				});

				var documentDatabase = server.Server.GetDatabaseInternal("mydb");
				documentDatabase.Wait();
				Assert.Equal("Pass", documentDatabase.Result.Configuration.Settings["Secret"]);
			}
		}
	}
}