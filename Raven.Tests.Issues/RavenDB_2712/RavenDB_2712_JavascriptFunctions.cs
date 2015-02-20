// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2712_JavascriptFunctions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Extensions;
using Raven.Database.Json;
using Raven.Json.Linq;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class RavenDB_2712_JavascriptFunctions : GlobalConfigurationTest
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

				var document = retriever.GetConfigurationDocument<RavenJObject>(Constants.RavenJavascriptFunctions);

				Assert.Null(document);

				systemDatabase
					.Documents
					.Put(
						Constants.Global.JavascriptFunctions,
						null,
						RavenJObject.FromObject(new { Functions = "exports.test = function(value) { return 'global ' + value; };" }),
						new RavenJObject(), null);

				document = retriever.GetConfigurationDocument<RavenJObject>(Constants.RavenJavascriptFunctions);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.False(document.LocalExists);
				Assert.Equal(Constants.RavenJavascriptFunctions, document.MergedDocument.Value<RavenJObject>("@metadata").Value<string>("@id"));
                Assert.Equal("exports.test = function(value) { return 'global ' + value; };", document.MergedDocument.Value<string>("Functions"));
			}
		}

		[Fact]
		public void PatcherShouldBeAwareOfGlobalAndLocalFunctions()
		{
			using (var store = NewRemoteDocumentStore(databaseName: "Northwind"))
			{
				var server = servers[0];
				var systemDatabase = server.SystemDatabase;
				var database = server.Server.GetDatabaseInternal("Northwind").ResultUnwrap();

				systemDatabase
				.Documents
				.Put(
					Constants.Global.JavascriptFunctions,
					null,
					RavenJObject.FromObject(new
					{
						Functions = "exports.onlyGlobal = function(value) { return 'onlyGlobal ' + value; };"
						 + "exports.override = function(value) { return 'override ' + value; };"
					}),
					new RavenJObject(), null);

				database
				.Documents
				.Put(
					Constants.RavenJavascriptFunctions,
					null,
					RavenJObject.FromObject(new
					{
						Functions = "exports.onlyLocal = function(value) { return 'onlyLocal ' + value; };"
						 + "exports.override = function(value) { return 'overrideLocal ' + value; };"
					}),
					new RavenJObject(), null);


				database.Documents.Put("doc/1", null, RavenJObject.FromObject(new { X = "1" }), new RavenJObject(), null);
				database.Documents.Put("doc/2", null, RavenJObject.FromObject(new { X = "2" }), new RavenJObject(), null);
				database.Documents.Put("doc/3", null, RavenJObject.FromObject(new { X = "3" }), new RavenJObject(), null);

				store.DatabaseCommands.Patch("doc/1", new ScriptedPatchRequest
				{
					Script = "this.a = onlyLocal(5)"
				});

				Assert.Equal("onlyLocal 5", store.DatabaseCommands.Get("doc/1").DataAsJson.Value<string>("a"));

				store.DatabaseCommands.Patch("doc/2", new ScriptedPatchRequest
				{
					Script = "this.a = onlyGlobal(5)"
				});

				Assert.Equal("onlyGlobal 5", store.DatabaseCommands.Get("doc/2").DataAsJson.Value<string>("a"));

				store.DatabaseCommands.Patch("doc/3", new ScriptedPatchRequest
				{
					Script = "this.a = override(5)"
				});

				Assert.Equal("overrideLocal 5", store.DatabaseCommands.Get("doc/3").DataAsJson.Value<string>("a"));
				

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

				var document = retriever.GetConfigurationDocument<RavenJObject>(Constants.RavenJavascriptFunctions);

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

				document = retriever.GetConfigurationDocument<RavenJObject>(Constants.RavenJavascriptFunctions);

				Assert.NotNull(document);
				Assert.True(document.GlobalExists);
				Assert.True(document.LocalExists);
                Assert.Equal(Constants.RavenJavascriptFunctions, document.MergedDocument.Value<RavenJObject>("@metadata").Value<string>("@id"));
				Assert.Equal("exports.test = function(value) { return 'global ' + value; };;exports.test = function(value) { return 'local ' + value; };", document.MergedDocument.Value<string>("Functions"));

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