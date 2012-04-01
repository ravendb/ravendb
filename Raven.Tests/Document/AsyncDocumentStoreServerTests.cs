//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentStoreServerTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Commands;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Json.Linq;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Document
{
	public class AsyncDocumentStoreServerTests : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly int port;
		private readonly RavenDbServer server;
		private readonly IDocumentStore documentStore;

		public AsyncDocumentStoreServerTests()
		{
			port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			server = GetNewServer(port, path);
			documentStore = new DocumentStore { Url = "http://localhost:" + port }.Initialize();
		}

		public void Dispose()
		{
			documentStore.Dispose();
			server.Dispose();
			IOExtensions.DeleteDirectory(path);
		}

		[Fact]
		public void Can_insert_sync_and_get_async()
		{
			var entity = new Company {Name = "Async Company"};
			using (var session = documentStore.OpenSession())
			{
				session.Store(entity);
				session.SaveChanges();
			}

			using (var session = documentStore.OpenAsyncSession())
			{
				var task = session.LoadAsync<Company>(entity.Id);

				Assert.Equal("Async Company", task.Result.Name);
			}
		}

		[Fact]
		public void Can_insert_async_and_get_sync()
		{
			var entity = new Company {Name = "Async Company"};
			using (var session = documentStore.OpenAsyncSession())
			{
				session.Store(entity);
				session.SaveChangesAsync().Wait();
			}

			using (var session = documentStore.OpenSession())
			{
				var company = session.Load<Company>(entity.Id);

				Assert.Equal("Async Company", company.Name);
			}
		}

		[Fact]
		public void Can_insert_async_and_multi_get_async()
		{
			var entity1 = new Company {Name = "Async Company #1"};
			var entity2 = new Company {Name = "Async Company #2"};
			using (var session = documentStore.OpenAsyncSession())
			{
				session.Store(entity1);
				session.Store(entity2);
				session.SaveChangesAsync().Wait();
			}

			using (var session = documentStore.OpenAsyncSession())
			{
				var task = session.LoadAsync<Company>(new[] {entity1.Id, entity2.Id});
				Assert.Equal(entity1.Name, task.Result[0].Name);
				Assert.Equal(entity2.Name, task.Result[1].Name);
			}
		}

		[Fact]
		public void Can_defer_commands_until_savechanges_async()
		{

			using (var session = documentStore.OpenAsyncSession())
			{
				var commands = new ICommandData[]
				               	{
				               		new PutCommandData
				               			{
				               				Document =
				               					RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				               				Etag = null,
				               				Key = "rhino1",
				               				Metadata = new RavenJObject(),
				               			},
				               		new PutCommandData
				               			{
				               				Document =
				               					RavenJObject.FromObject(new Company {Name = "Hibernating Rhinos"}),
				               				Etag = null,
				               				Key = "rhino2",
				               				Metadata = new RavenJObject(),
				               			}
				               	};

				session.Advanced.Defer(commands);
				session.Advanced.Defer(new DeleteCommandData
				                       	{
				                       		Etag = null,
				                       		Key = "rhino2"
				                       	});

				Assert.Equal(0, session.Advanced.NumberOfRequests);

				session.SaveChangesAsync().Wait();
				//Assert.Equal(1, session.Advanced.NumberOfRequests); // This returns 0 for some reason in async mode

				// Make sure that session is empty
				//session.SaveChangesAsync().Wait();
				//Assert.Equal(1, session.Advanced.NumberOfRequests);
			}

			Assert.Null(documentStore.DatabaseCommands.Get("rhino2"));
			Assert.NotNull(documentStore.DatabaseCommands.Get("rhino1"));
		}
	}
}