//-----------------------------------------------------------------------
// <copyright file="QueryWithPercentageSignp.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Server;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class QueryWithPercentageSignp : RemoteClientTest, IDisposable
	{
		private readonly string path;
		private readonly RavenDbServer ravenDbServer;
		private readonly IDocumentStore documentStore;

		public QueryWithPercentageSignp()
		{
			const int port = 8079;
			path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8079);

			ravenDbServer = GetNewServer(port, path);
			documentStore = new DocumentStore { Url = "http://localhost:" + port }.Initialize();
		}

		public override void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			IOExtensions.DeleteDirectory(path);
			base.Dispose();
		}

		[Fact]
		public void CanQueryUsingPercentageSign()
		{
			documentStore.DatabaseCommands.PutIndex("Tags/Count",
			                                new IndexDefinition
			                                	{
			                                		Map = "from tag in docs.Tags select new { tag.Name, tag.UserId }"
			                                	});

			using (var session = documentStore.OpenSession())
			{
				var userId = "users/1";
				var tag = "24%";
				session.Query<TagCount>("Tags/Count").FirstOrDefault(x => x.Name == tag && x.UserId == userId);
			}
		}

		public class TagCount
		{
			public string Name { get; set; }
			public string UserId { get; set; }
		}
	}
}