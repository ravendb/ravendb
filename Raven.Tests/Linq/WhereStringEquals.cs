using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Linq
{
	public class WhereStringEquals : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		public WhereStringEquals()
		{
			store = NewDocumentStore();
			using (var session = store.OpenSession())
			{
				session.Store(new MyEntity {StringData = "Some data"});
				session.Store(new MyEntity {StringData = "Some DATA"});
				session.Store(new MyEntity {StringData = "Some other data"});
				session.SaveChanges();
			}
		}

		[Fact]
		public void QueryString_CaseSensitive_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
                Assert.Throws<NotSupportedException>(() =>
                    session.Query<MyEntity>()
                    .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                    .Count(o => string.Equals(o.StringData, "Some data", StringComparison.InvariantCulture)));
			}
		}

		[Fact]
		public void QueryString_IgnoreCase_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => string.Equals(o.StringData, "Some data", StringComparison.InvariantCultureIgnoreCase));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => string.Equals(o.StringData, "Some data"));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork_InvertParametersOrder()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => string.Equals("Some data", o.StringData));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void RegularStringEqual_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => "Some data" == o.StringData);

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void ConstantStringEquals_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => "Some data".Equals(o.StringData));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void StringEqualsConstant_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => o.StringData.Equals("Some data"));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void StringEqualsConstant_IgnoreCase_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
				var count = session.Query<MyEntity>()
					.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
					.Count(o => "Some data".Equals(o.StringData, StringComparison.OrdinalIgnoreCase));

				Assert.Equal(2, count);
			}
		}

		[Fact]
		public void StringEqualsConstant_CaseSensitive_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
			    Assert.Throws<NotSupportedException>(() =>
                    session.Query<MyEntity>()
			        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                    .Count(o => "Some data".Equals(o.StringData, StringComparison.Ordinal)));

			}
		}

		[Fact]
		public void RegularStringEqual_CaseSensitive_ShouldWork()
		{
			using (var session = store.OpenSession())
			{
			    Assert.Throws<NotSupportedException>(() =>
                    session.Query<MyEntity>()
                    .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                    .Count(o => o.StringData.Equals("Some data", StringComparison.Ordinal)));
			}
		}

		public class MyEntity
		{
			public string StringData { get; set; }
		}
	}
}