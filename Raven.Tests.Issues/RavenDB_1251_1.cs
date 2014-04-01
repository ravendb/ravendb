using System;
using System.Diagnostics;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1251_1 : RavenTestBase
	{
		public class Foo
		{
			public TimeSpan Bar { get; set; }
		}

		[Fact]
		public void TimeSpan_Can_Sort_By_Range_Value()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Bar = TimeSpan.FromHours(-2) });
					session.Store(new Foo { Bar = TimeSpan.FromHours(-1) });
					session.Store(new Foo { Bar = TimeSpan.FromHours(0) });
					session.Store(new Foo { Bar = TimeSpan.FromHours(1) });
					session.Store(new Foo { Bar = TimeSpan.FromHours(2) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var t = TimeSpan.FromHours(-1.5);

					var q = session.Query<Foo>()
								   .Customize(x => x.WaitForNonStaleResults())
								   .Where(x => x.Bar > t)
								   .OrderByDescending(x => x.Bar);
					Debug.WriteLine(q);
					var result = q.ToList();
					
					Assert.Equal(4, result.Count);
					Assert.True(result[0].Bar > result[1].Bar);
					Assert.True(result[1].Bar > result[2].Bar);
					Assert.True(result[2].Bar > result[3].Bar);
				}
			}
		}
	}
}
