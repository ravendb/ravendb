using Raven.Imports.Newtonsoft.Json;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class SerializingAndDeserializingWithRaven
	{
		[Fact]
		public void can_deserialize_id_with_private_setter()
		{
			using (var documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			})
			{

				documentStore.Initialize();

				using (var session = documentStore.OpenSession())
				{
					var testObj = new TestObj(1000, 123);
					session.Store(testObj);
					session.SaveChanges();
					session.Advanced.Clear();
					var load = session.Load<TestObj>(1000);

					Assert.Equal(123, load.AnotherLong);
					Assert.Equal(1000, load.Id);
				}
			}
		}

		public abstract class AggregateRoot
		{
			protected AggregateRoot()
			{
			}

			protected AggregateRoot(long id, long anotherLong)
			{
				Id = id;
				AnotherLong = anotherLong;
			}

			public long Id { get; private set; }
			public long AnotherLong { get; private set; }
		}

		public class TestObj : AggregateRoot
		{
			[JsonConstructor]
			public TestObj()
			{
			}

			public TestObj(long id, long anotherLong)
				: base(id,
					anotherLong)
			{
			}
		}
	}
}