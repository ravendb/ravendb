using System;
using System.Linq;
using Raven.Abstractions;
using Xunit;

namespace Raven.Tests.Linq
{
	public class Any : RavenTest
	{
		private class TestDoc
		{
			public string SomeProperty { get; set; }
			public string[] StringArray { get; set; }
		}

		[Fact]
		public void CanQueryArrayWithAny()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var doc = new TestDoc {StringArray = new [] {"test", "doc", "foo"}};
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

		[Fact]
		public void CanCountWithAny()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc {StringArray = new[] {"one", "two"}});
					session.Store(new TestDoc {StringArray = new string[0]});
					session.Store(new TestDoc {StringArray = new string[0]});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(1, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any()));
				}
			}
		}

		[Fact]
		public void CannotCountNullArraysWithAnyIfThereIsNothingElseStoredInTheIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc { StringArray = new[] { "one", "two" } });
					session.Store(new TestDoc { StringArray = new string[0] });
					session.Store(new TestDoc { StringArray = new string[0] });
					session.SaveChanges();
				}
				WaitForUserToContinueTheTest(store);
				using (var session = store.OpenSession())
				{
					Assert.Equal(0, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any() == false));
				}
			}
		}

		[Fact]
		public void CanCountNullArraysWithAnyIfHaveAnotherPropertyStoredInTheIndex()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new TestDoc {SomeProperty = "Test", StringArray = new[] {"one", "two"}});
					session.Store(new TestDoc {SomeProperty = "Test", StringArray = new string[0]});
					session.Store(new TestDoc {SomeProperty = "Test", StringArray = new string[0]});
					session.SaveChanges();
				}
				WaitForUserToContinueTheTest(store);
				using (var session = store.OpenSession())
				{
					Assert.Equal(2, session.Query<TestDoc>().Customize(customization => customization.WaitForNonStaleResults()).Count(p => p.StringArray.Any() == false && p.SomeProperty == "Test"));
				}
			}
		}

		private class OrderableEntity
		{
			public DateTime Order { get; set; }
		}

		[Fact]
		public void NullRefWhenQuerying()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					DateTime dateTime = SystemTime.UtcNow;
					var query = from a in session.Query<OrderableEntity>()
													.Customize(x => x.WaitForNonStaleResultsAsOfNow())
								where dateTime < a.Order
								select a;

					query.ToList();

				}
			}
		}
	}
}