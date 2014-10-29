using System;
using System.Linq;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTime_QueryMapReduceTests : RavenTest
	{
		[Fact]
		public void DateTime_SavedAs_Local_OnMapReduceIndex_QueriesAs_Unspecified()
		{
			// There is no representation of Local once the DateTime is serialized,
			// so this is the one case where the date does not come back exactly as it was saved.
			// This is a known and expected behavior of DateTime.

			DoTest(DateTime.Now, DateTimeKind.Local, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_Unspecified_OnMapReduceIndex_QueriesAs_Unspecified()
		{
			DoTest(new DateTime(2012, 1, 1), DateTimeKind.Unspecified, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_UTC_OnMapReduceIndex_QueriesAs_UTC()
		{
			DoTest(DateTime.UtcNow, DateTimeKind.Utc, DateTimeKind.Utc);
		}

		private void DoTest(DateTime dt, DateTimeKind inKind, DateTimeKind outKind)
		{
			Assert.Equal(inKind, dt.Kind);

			using (var documentStore = NewDocumentStore())
			{
				new Foos_MinAndMaxDateTime().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTime = dt.AddDays(-1) });
					session.Store(new Foo { Id = "foos/2", DateTime = dt });
					session.Store(new Foo { Id = "foos/3", DateTime = dt.AddDays(1) });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Foos_MinAndMaxDateTime.Result, Foos_MinAndMaxDateTime>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .First();

					Assert.Equal(dt.AddDays(-1), results.MinDateTime);
					Assert.Equal(dt.AddDays(1), results.MaxDateTime);

					Assert.Equal(outKind, results.MinDateTime.Kind);
					Assert.Equal(outKind, results.MaxDateTime.Kind);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTime DateTime { get; set; }
		}

		public class Foos_MinAndMaxDateTime : AbstractIndexCreationTask<Foo, Foos_MinAndMaxDateTime.Result>
		{
			public class Result
			{
				public DateTime MinDateTime { get; set; }
				public DateTime MaxDateTime { get; set; }
			}

			public Foos_MinAndMaxDateTime()
			{
				Map = foos => from foo in foos
							  select new
								  {
									  MinDateTime = foo.DateTime,
									  MaxDateTime = foo.DateTime
								  };

				Reduce = results => from result in results
									group result by 0
										into g
										select new
											{
												MinDateTime = g.Min(x => x.MinDateTime),
												MaxDateTime = g.Max(x => x.MaxDateTime)
											};
			}
		}
	}
}
