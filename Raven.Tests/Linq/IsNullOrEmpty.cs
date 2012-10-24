using System.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class IsNullOrEmpty : RavenTest
	{
		private class TestDoc
		{
			public string SomeProperty { get; set; }
		}

		[Fact]
		public void IsNullOrEmpty_True()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc { SomeProperty = "Has some content" });
					session.Store(new TestDoc { SomeProperty = "" });
					session.Store(new TestDoc { SomeProperty = null });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(2, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty)));
				}

				WaitForUserToContinueTheTest(store);
			}
		}

		[Fact]
		public void IsNullOrEmpty_False()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc { SomeProperty = "Has some content" });
					session.Store(new TestDoc { SomeProperty = "" });
					session.Store(new TestDoc { SomeProperty = null });
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty) == false));
				}
			}
		}

		[Fact]
		public void IsNullOrEmpty_WithExclamationMark()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc { SomeProperty = "Has some content" });
					session.Store(new TestDoc { SomeProperty = "" });
					session.Store(new TestDoc { SomeProperty = null });
					session.SaveChanges();
				}
				
				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TestDoc>().Count(p => !string.IsNullOrEmpty(p.SomeProperty)));
				}
			}
		}
	}
}