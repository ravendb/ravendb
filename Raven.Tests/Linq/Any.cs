
using System.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class Any : LocalClientTest
	{
		private class TestDoc
		{
			public string SomeProperty { get; set; }
			public string[] StringArray { get; set; }
		}

		[Fact]
		public void CanQueryArray()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var doc = new TestDoc {StringArray = new string[] {"test", "doc", "foo"}};
					session.Store(doc);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var otherDoc = new TestDoc {SomeProperty = "foo"};
					var doc = (from ar in session.Query<TestDoc>()
							   where ar.StringArray.Any(ac => ac == otherDoc.SomeProperty)
							   select ar).FirstOrDefault();
					Assert.NotNull(doc);
				}
			}
		}
	}
}
