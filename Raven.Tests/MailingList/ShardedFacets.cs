// -----------------------------------------------------------------------
//  <copyright file="ShardedFacets.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class ShardedFacets : RavenTestBase
	{
		[Fact]
		public void FacetTest()
		{
			using (GetNewServer(8079, dataDirectory:"Data1"))
			using (GetNewServer(8078, dataDirectory: "Data2"))
			using (var ds1 = CreateDocumentStore(8079))
			using (var ds2 = CreateDocumentStore(8078))
			{
				var sharded = new ShardedDocumentStore(
					new ShardStrategy(
						new Dictionary<string, IDocumentStore> {
                        {"first", ds1},
                        {"second", ds2}
                    }));

				using (sharded.Initialize())
				{
					using (var session = sharded.OpenSession())
					{
						session.Store(new Tag { Name = "tag1" });
						session.Store(new Tag { Name = "tag1" });
						session.Store(new Tag { Name = "tag2" });
						session.SaveChanges();
					}

					using (var session = sharded.OpenSession())
					{
						session.Store(new Tag { Name = "tag3" });
						session.Store(new Tag { Name = "tag5" });
						session.Store(new Tag { Name = "tag8" });
						session.SaveChanges();
					}

					new Tags_ByName().Execute(sharded);

					
					WaitForIndexing(ds1);
					WaitForIndexing(ds2);

					using (var session = sharded.OpenSession())
						Assert.NotEmpty(session
							.Query<Tag, Tags_ByName>()
							.ToFacets(new[] { new Facet { Name = "Name" } }).Results);
				}
			}
		}

		private static IDocumentStore CreateDocumentStore(int port)
		{
			return new DocumentStore
			{
				Url = string.Format("http://localhost:{0}/", port),
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.FailImmediately
				}
			};
		}

		public class Tags_ByName : AbstractIndexCreationTask<Tag>
		{
			public Tags_ByName()
			{
				Map = tags =>
				      from tag in tags
				      select new {tag.Name};
			}
		}

		public class Tag
		{
			public string Name { get; set; }
		}
	}
}