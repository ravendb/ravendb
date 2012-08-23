using System.Linq;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
	public class Floats : RavenTest
	{
		public class FloatValue
		{
			public int Id { get; set; }
			public float Value { get; set; }
		}

		[Fact]
		public void Query()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new FloatValue
					              	{
					              		Id = 1,
					              		Value = 3.3f
					              	});
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{

					var results = session.Query<FloatValue>()
						.Where(x => x.Value == 3.3f)
						.Customize(x => x.WaitForNonStaleResults());

					Assert.True(results.Count() == 1);
				}
			}
		}

		[Fact]
		public void Persistence()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new FloatValue
					{
						Id = 1,
						Value = 3.3f
					});
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var value = session.Load<FloatValue>(1);
					Assert.Equal(3.3f, value.Value);
				}
			}
		}
	}
}
