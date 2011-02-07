using System.Dynamic;
using System.Linq;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Client;
using Raven.Database.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class LuceneQueryCustomMetadata
	{
		private const string PropertyName = "MyCustomProperty";

		[Fact]
		public void SuccessTest1()
		{
			using (IDocumentStore documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				dynamic expando = new ExpandoObject();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					session.Store(expando);

					JObject metadata =
						session.Advanced.GetMetadataFor((ExpandoObject)expando);

					metadata[PropertyName] = JToken.FromObject(true);

					session.SaveChanges();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var loaded =
						session.Load<dynamic>((string)expando.Id);

					JObject metadata =
						session.Advanced.GetMetadataFor((DynamicJsonObject)loaded);
					JToken token = metadata[PropertyName];

					Assert.NotNull(token);
					Assert.True(token.Value<bool>());
				}
			}
		}

		[Fact]
		public void SuccessTest2()
		{
			using (IDocumentStore documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				dynamic expando = new ExpandoObject();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					session.Store(expando);

					JObject metadata =
						session.Advanced.GetMetadataFor((ExpandoObject)expando);

					metadata[PropertyName] = JToken.FromObject(true);

					session.SaveChanges();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					dynamic loaded = session.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("@metadata.Raven-Entity-Name",
									 documentStore.Conventions.GetTypeTagName(typeof(ExpandoObject)))
						.FirstOrDefault();

					Assert.NotNull(loaded);
				}
			}
		}

		[Fact]
		public void FailureTest()
		{
			using (IDocumentStore documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize())
			{
				dynamic expando = new ExpandoObject();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					session.Store(expando);

					JObject metadata =
						session.Advanced.GetMetadataFor((ExpandoObject)expando);

					metadata[PropertyName] = JToken.FromObject(true);

					session.SaveChanges();
				}

				using (IDocumentSession session = documentStore.OpenSession())
				{
					dynamic loaded =
						session.Advanced.LuceneQuery<dynamic>().WhereEquals("@metadata." + PropertyName, true)
							.FirstOrDefault();

					Assert.NotNull(loaded);
				}
			}
		}
	}
}