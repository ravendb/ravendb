using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTime_QueryMultiMapTests : RavenTest
	{
		[Fact]
		public void DateTime_SavedAs_Local_OnMultiMapIndex_QueriesAs_Unspecified()
		{
			// There is no representation of Local once the DateTime is serialized,
			// so this is the one case where the date does not come back exactly as it was saved.
			// This is a known and expected behavior of DateTime.

			DoTest(DateTime.Now, DateTimeKind.Local, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_Unspecified_OnMultiMapIndex_QueriesAs_Unspecified()
		{
			DoTest(new DateTime(2012, 1, 1), DateTimeKind.Unspecified, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_UTC_OnMultiMapIndex_QueriesAs_UTC()
		{
			DoTest(DateTime.UtcNow, DateTimeKind.Utc, DateTimeKind.Utc);
		}

		private void DoTest(DateTime dt, DateTimeKind inKind, DateTimeKind outKind)
		{
			Assert.Equal(inKind, dt.Kind);

			using (var documentStore = NewDocumentStore(runInMemory: true))
			{
				documentStore.Initialize();
				new FoosAndBars_ByDateTime().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTime1 = dt });
					session.Store(new Bar { Id = "bars/1", DateTime2 = dt });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<FoosAndBars_ByDateTime.Result, FoosAndBars_ByDateTime>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.DateTime == dt)
										 .As<object>()
										 .ToList();

					var foo = results.OfType<Foo>().First();
					var bar = results.OfType<Bar>().First();

					Assert.Equal(dt, foo.DateTime1);
					Assert.Equal(outKind, foo.DateTime1.Kind);

					Assert.Equal(dt, bar.DateTime2);
					Assert.Equal(outKind, bar.DateTime2.Kind);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public DateTime DateTime1 { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public DateTime DateTime2 { get; set; }
		}

		public class FoosAndBars_ByDateTime : AbstractMultiMapIndexCreationTask<FoosAndBars_ByDateTime.Result>
		{
			public class Result
			{
				public DateTime DateTime { get; set; }
			}

			public FoosAndBars_ByDateTime()
			{
				AddMap<Foo>(foos => from foo in foos
									select new { DateTime = foo.DateTime1 });

				AddMap<Bar>(bars => from bar in bars
									select new { DateTime = bar.DateTime2 });
			}
		}
	}
}