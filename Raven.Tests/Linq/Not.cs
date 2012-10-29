using System.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class Not : RavenTest
	{
		private class TestDoc
		{
			public string SomeProperty { get; set; }
		}

		[Fact]
		public void CanQueryWithNot()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc {SomeProperty = "NOT"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "NOT"));
				}
			}
		}
	}
}