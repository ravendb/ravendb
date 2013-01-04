using System;
using System.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class Contains : RavenTest
	{
		private class TestDoc
		{
			public string SomeProperty { get; set; }
			public string[] StringArray { get; set; }
		}

		[Fact]
		public void CanQueryArrayWithContains()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var doc = new TestDoc {StringArray = new[] {"test", "doc", "foo"}};
					session.Store(doc);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var otherDoc = new TestDoc {SomeProperty = "foo"};
					var doc = session.Query<TestDoc>()
						.FirstOrDefault(ar => ar.StringArray.Contains(otherDoc.SomeProperty));
					Assert.NotNull(doc);
				}
			}
		}

		[Fact]
		public void DoesNotSupportStrings()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var doc = new TestDoc {SomeProperty = "Ensure that Contains on IEnumerable<Char> is not supported."};
					session.Store(doc);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var otherDoc = new TestDoc {SomeProperty = "Contains"};
					var exception = Assert.Throws<NotSupportedException>(() =>
					{
						session.Query<TestDoc>().FirstOrDefault(ar => ar.SomeProperty.Contains(otherDoc.SomeProperty));
					});
					Assert.Contains("Contains is not supported, doing a substring match", exception.Message);
				}
			}
		}
	}
}