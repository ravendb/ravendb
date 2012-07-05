using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class Generics : LocalClientTest
	{
		public class MyClass<T>
		{
			public string Id { get; set; }
			public T Child { get; set; }
		}

		public void Save<T>(string id, T child)
		{
			using (var dataStore = NewDocumentStore())
			{
				using (var session = dataStore.OpenSession())
				{
					var obj = new MyClass<T>
								{
									Id = id,
									Child = child
								};
					session.Store(obj);
					session.SaveChanges();
				}
			}
		}

		[Fact]
		public void CanSaveWithGenerics()
		{
			Save("myId", false);
		}

		[Fact]
		public void TestWithoutGenerics()
		{
			using (var dataStore = NewDocumentStore())
			{
				using (var session = dataStore.OpenSession())
				{
					var node = new TestNode
								{
									Id = 12345,
									Latitude = 51.1f,
									Longitude = 52.2f
								};

					session.Store(node);
					session.SaveChanges();
				}
			}
		}

		public class TestNode
		{
			public int Id { get; set; }
			public float Latitude { get; set; }
			public float Longitude { get; set; }
		}
	}
}
