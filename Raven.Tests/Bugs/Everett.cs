using System;
using System.IO;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class Everett : IDisposable
	{
		private readonly EmbeddableDocumentStore _documentStore;

		public class TestEntity
		{
			public string Id { get; set; }
			public DateTime LastSavedDate { get; set; }
		}

		public Everett()
		{
			_documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true,
				Conventions =
				{
					CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All,
					DefaultQueryingConsistency = ConsistencyOptions.QueryYourWrites,
				}
			};
			_documentStore.Initialize();
		}

		public void Dispose()
		{
			_documentStore.Dispose();
		}

		[Fact]
		public void Test()
		{
			using (var session = _documentStore.OpenSession())
			{
				TestEntity entity = new TestEntity { LastSavedDate = DateTime.Now };
				session.Store(entity);
				session.SaveChanges();
				entity = session.Include("RelatedEntityId").Load<TestEntity>(entity.Id);
			}
		}
	}
}