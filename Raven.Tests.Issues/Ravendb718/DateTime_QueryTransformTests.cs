using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues.Ravendb718
{
	public class DateTime_QueryTransformTests : RavenTest
	{
		[Fact]
		public void DateTime_SavedAs_Local_OnTransformIndex_QueriesAs_Unspecified()
		{
			// There is no representation of Local once the DateTime is serialized,
			// so this is the one case where the date does not come back exactly as it was saved.
			// This is a known and expected behavior of DateTime.

			DoTest(DateTime.Now, DateTimeKind.Local, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_Unspecified_OnTransformIndex_QueriesAs_Unspecified()
		{
			DoTest(new DateTime(2012, 1, 1), DateTimeKind.Unspecified, DateTimeKind.Unspecified);
		}

		[Fact]
		public void DateTime_SavedAs_UTC_OnTransformIndex_QueriesAs_UTC()
		{
			DoTest(DateTime.UtcNow, DateTimeKind.Utc, DateTimeKind.Utc);
		}

		private void DoTest(DateTime dt, DateTimeKind inKind, DateTimeKind outKind)
		{
			Assert.Equal(inKind, dt.Kind);

			using (var documentStore = NewDocumentStore())
			{
				new FoosAndBars().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "foos/1", DateTime = dt, BarId = "bars/1" });
					session.Store(new Bar { Id = "bars/1", DateTime = dt });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<Foo, FoosAndBars>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.DateTime == dt)
										 .As<FooAndBar>()
										 .First();

					Assert.Equal(dt, results.FooDateTime);
					Assert.Equal(dt, results.BarDateTime);

					Assert.Equal(outKind, results.FooDateTime.Kind);
					Assert.Equal(outKind, results.BarDateTime.Kind);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string BarId { get; set; }
			public DateTime DateTime { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public DateTime DateTime { get; set; }
		}

		public class FooAndBar
		{
			public string FooId { get; set; }
			public string BarId { get; set; }
			public DateTime FooDateTime { get; set; }
			public DateTime BarDateTime { get; set; }
		}

		public class FoosAndBars : AbstractIndexCreationTask<Foo>
		{
			public FoosAndBars()
			{
				Map = foos => from foo in foos
							  select new
								  {
									  foo.DateTime,
								  };

				TransformResults = (database, foos) => from foo in foos
													   let bar = database.Load<Bar>(foo.BarId)
													   select new
														   {
															   FooId = foo.Id,
															   BarId = bar.Id,
															   FooDateTime = foo.DateTime,
															   BarDateTime = bar.DateTime
														   };
			}
		}
	}
}
