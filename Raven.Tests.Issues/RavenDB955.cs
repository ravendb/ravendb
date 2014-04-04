using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB955 : RavenTest
	{
		[Fact]
		public void CanQueryWithNullComparison()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new WithNullableField { TheNullableField = 1 });
					s.Store(new WithNullableField { TheNullableField = null });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{

					Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField == null));
					Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField != null));
				}
			}

		}

		[Fact]
		public void CanQueryWithHasValue()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new WithNullableField { TheNullableField = 1 });
					s.Store(new WithNullableField { TheNullableField = null });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => !x.TheNullableField.HasValue));
					Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField.HasValue));
				}
			}

		}

		public class WithNullableField
		{
			public int? TheNullableField
			{ get; set; }
		}
	}
}