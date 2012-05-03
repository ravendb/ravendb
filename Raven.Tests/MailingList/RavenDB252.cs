using Lucene.Net.Util;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Json.Linq;
using Xunit;
using Constants = Raven.Abstractions.Data.Constants;
using System.Linq;

namespace Raven.Tests.MailingList
{
	public class RavenDB252 : RavenTest
	{
		[Fact]
		public void EntityNameIsNowCaseInsensitive()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("a", null, new RavenJObject
				{
					{"FirstName", "Oren"}
				}, new RavenJObject
				{
					{Constants.RavenEntityName, "Users"}
				});

				store.DatabaseCommands.Put("b", null, new RavenJObject
				{
					{"FirstName", "Ayende"}
				}, new RavenJObject
				{
					{Constants.RavenEntityName, "users"}
				});

				using(var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<User>().Where(x=>x.FirstName == "Oren"));

					Assert.NotEmpty(session.Query<User>().Where(x => x.FirstName == "Ayende"));
				}
			}
		}
		
		[Fact]
		public void EntityNameIsNowCaseInsensitive_Method()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put("a", null, new RavenJObject
				{
					{"FirstName", "Oren"}
				}, new RavenJObject
				{
					{Constants.RavenEntityName, "Users"}
				});

				store.DatabaseCommands.Put("b", null, new RavenJObject
				{
					{"FirstName", "Ayende"}
				}, new RavenJObject
				{
					{Constants.RavenEntityName, "users"}
				});

				store.Conventions.DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites;

				store.DatabaseCommands.PutIndex("UsersByName", new IndexDefinition
				{
					Map = "docs.users.Select(x=>new {x.FirstName })"
				});

				using (var session = store.OpenSession())
				{
					Assert.NotEmpty(session.Query<User>("UsersByName").Where(x => x.FirstName == "Oren"));

					Assert.NotEmpty(session.Query<User>("UsersByName").Where(x => x.FirstName == "Ayende"));
				}
			}
		}
	}
}