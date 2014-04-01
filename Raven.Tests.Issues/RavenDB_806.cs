using System;
using System.Diagnostics;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_806 : RavenTestBase
	{
		[Fact]
		public void TimeSpan_Can_Get_Range_Under_A_Day()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(20) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var time = TimeSpan.FromHours(15);
					var result = session.Query<Foo>()
										.Customize(x => x.WaitForNonStaleResults())
										.SingleOrDefault(x => x.Start <= time && x.Until > time);
					Assert.NotNull(result);
				}
			}
		}

		[Fact]
		public void TimeSpan_Can_Get_Range_Over_A_Day()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromHours(30), Until = TimeSpan.FromHours(40) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var time = TimeSpan.FromHours(35);
					var result = session.Query<Foo>()
										.Customize(x => x.WaitForNonStaleResults())
										.SingleOrDefault(x => x.Start <= time && x.Until > time);

					Assert.NotNull(result);
				}
			}
		}

		[Fact]
		public void TimeSpan_Can_Get_Range_Mixed_Days()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromHours(20), Until = TimeSpan.FromHours(30) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var time = TimeSpan.FromHours(25);
					var result = session.Query<Foo>()
										.Customize(x => x.WaitForNonStaleResults())
										.SingleOrDefault(x => x.Start <= time && x.Until > time);

					Assert.NotNull(result);
				}
			}
		}

		[Fact]
		public void TimeSpan_Can_Get_Range_VeryLarge()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromDays(100) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var time = TimeSpan.FromDays(2);
					var result = session.Query<Foo>()
										.Customize(x => x.WaitForNonStaleResults())
										.SingleOrDefault(x => x.Start <= time && x.Until > time);

					Assert.NotNull(result);
				}
			}
		}

		[Fact]
		public void TimeSpan_Can_Get_Range_Negatives()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromHours(-10), Until = TimeSpan.FromHours(10) });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var time = TimeSpan.FromHours(1);
					var result = session.Query<Foo>()
										.Customize(x => x.WaitForNonStaleResults())
										.SingleOrDefault(x => x.Start <= time && x.Until > time);

					WaitForUserToContinueTheTest(documentStore);
					Assert.NotNull(result);
				}
			}
		}
		[Fact]
		public void Test()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Start = TimeSpan.FromDays(-4) });
					session.Store(new Foo { Start = TimeSpan.FromHours(-3) });
					session.Store(new Foo { Start = TimeSpan.Zero });
					session.Store(new Foo { Start = TimeSpan.FromHours(2) });
					session.Store(new Foo { Start = TimeSpan.FromDays(1) });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Foo>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .OrderBy(x => x.Start)
										 .ToList();

					foreach (var result in results)
						Debug.WriteLine(result.Start);

					for (int i = 1; i < results.Count; i++)
						Assert.True(results[i - 1].Start < results[i].Start,
									string.Format("Results are out of sequence. {0} < {1} failed.", results[i - 1].Start, results[i].Start));
				}
			}
		}

		public class Foo
		{
			public TimeSpan Start { get; set; }
			public TimeSpan Until { get; set; }
		}

	}
}