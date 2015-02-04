// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_JavascriptFunctions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_JavascriptFunctions : RavenTest
	{
		[Fact]
		public void GlobalConfigurationShouldBeEffectiveIfThereIsNoLocal()
		{
			using (NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<JsonDocument>(Constants.RavenJavascriptFunctions);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.JavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'global ' + value; };" }),
						new RavenJObject(), null);

				document = retriever.GetConfigurationDocument<JsonDocument>(Constants.RavenJavascriptFunctions);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
				Assert.Equal(Constants.RavenJavascriptFunctions, document.MergedDocument.Key);
                Assert.Equal("exports.test = function(value) { return 'global ' + value; };", document.MergedDocument.DataAsJson["Functions"]);
			}
		}

		[Fact]
		public void GlobalConfigurationShouldBeMergedWithLocalButLocalFunctionsShouldTakePrecedence()
		{
			using (var store = NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();
				var retriever = database.ConfigurationRetriever;

				var document = retriever.GetConfigurationDocument<JsonDocument>(Constants.RavenJavascriptFunctions);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.JavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'global ' + value; };" }),
						new RavenJObject(), null);

				database
					.Documents
					.Put(
						Constants.RavenJavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'local ' + value; };" }),
						new RavenJObject(), null);

				document = retriever.GetConfigurationDocument<JsonDocument>(Constants.RavenJavascriptFunctions);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.Equal(Constants.RavenJavascriptFunctions, document.MergedDocument.Key);
				Assert.Equal("exports.test = function(value) { return 'global ' + value; }; exports.test = function(value) { return 'local ' + value; };", document.MergedDocument.DataAsJson["Functions"]);

				using (var session = store.OpenSession())
				{
					session.Store(new Person
					{
						Name = "Name1"
					});

					session.SaveChanges();
				}

				store
					.DatabaseCommands
					.Patch("people/1", new ScriptedPatchRequest { Script = "this.Name = test(this.Name);" });

				using (var session = store.OpenSession())
				{
					var person = session.Load<Person>("people/1");
					Assert.Equal("local Name1", person.Name);
				}
			}
		}
	}
}