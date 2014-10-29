using System;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class Everett : RavenTest
	{
		public class TestEntity
		{
			public string Id { get; set; }
			public DateTime LastSavedDate { get; set; }
		}

		[Fact]
		public void Test()
		{
			using (var store = NewDocumentStore(configureStore: documentStore =>
			{
				documentStore.Conventions.CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All;
				documentStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites;
			}))
			using (var session = store.OpenSession())
			{
				TestEntity entity = new TestEntity { LastSavedDate = DateTime.Now };
				session.Store(entity);
				session.SaveChanges();
				entity = session.Include("RelatedEntityId")
				                .Load<TestEntity>(entity.Id);
			}
		}
	}
}