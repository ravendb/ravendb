using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class WhereInShouldBeBoostedCorrectly : RavenTest
	{
		public class OneLevelDeeper
		{
			public string ProfessionalTranslation { get; set; }
		}

		public class SampleData
		{
			public string Id { get; set; }
			public Dictionary<string, OneLevelDeeper> FirstName { get; set; }
			public Dictionary<string, OneLevelDeeper> LastName { get; set; }

			public SampleData()
			{
				FirstName = new Dictionary<string, OneLevelDeeper>();
				LastName = new Dictionary<string, OneLevelDeeper>();
			}
		}

		public class SampleDataIndex : AbstractIndexCreationTask<SampleData>
		{
			public SampleDataIndex()
			{
				Map = samples => from sample in samples
				                 select new
				                 {
					                 _ =
					                 sample.FirstName.Select(
						                 x => CreateField("FirstName_" + x.Key, x.Value.ProfessionalTranslation.Boost(10), true, true)),
					                 __ =
					                 sample.LastName.Select(
						                 x => CreateField("LastName_" + x.Key, x.Value.ProfessionalTranslation.Boost(9), true, true)),
				                 };
			}
		}

		[Fact]
		public void WhenUsingWhereBoostWorksFine()
		{
			using (var store = NewDocumentStore())
			{
				new SampleDataIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var mark = new SampleData {Id = "sample/mark"};
					mark.FirstName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Mark"};

					var bob1 = new SampleData {Id = "sample/bob1"};
					bob1.LastName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Bob"};

					var bob2 = new SampleData {Id = "sample/bob2"};
					bob2.FirstName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Bob"};

					session.Store(mark);
					session.Store(bob1);
					session.Store(bob2);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					//Removing this list and using .WhereEquals("FirstName_en", "Bob") works.

                    var results = session.Advanced.DocumentQuery<SampleData>(typeof(SampleDataIndex).Name)
					                     .WaitForNonStaleResults()
					                     .WhereEquals("FirstName_en", "Bob")
					                     .OrElse()
					                     .WhereEquals("LastName_en", "Bob")
					                     .ToList();

					//Bob2 should be returned as he has "Bob" in FirstName(boosted) whereas others do not.
					Assert.Equal(2, results.Count);
					Assert.Equal(results[0].Id, "sample/bob2");
					Assert.Equal(results[1].Id, "sample/bob1");
				}
			}
		}


		[Fact]
		public void WhenUsingWhereInBoostShouldAlsoTakePlace()
		{
			using (var store = NewDocumentStore())
			{
				new SampleDataIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var mark = new SampleData {Id = "sample/mark"};
					mark.FirstName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Mark"};

					var bob1 = new SampleData {Id = "sample/bob1"};
					bob1.LastName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Bob"};

					var bob2 = new SampleData {Id = "sample/bob2"};
					bob2.FirstName["en"] = new OneLevelDeeper {ProfessionalTranslation = "Bob"};

					session.Store(mark);
					session.Store(bob1);
					session.Store(bob2);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var list = new List<string> {"Bob"};

					//Removing this list and using .WhereEquals("FirstName_en", "Bob") works.

                    var results = session.Advanced.DocumentQuery<SampleData>(typeof(SampleDataIndex).Name)
					                     .WaitForNonStaleResults()
					                     .WhereIn("FirstName_en", list)
					                     .OrElse()
					                     .WhereIn("LastName_en", list)
					                     .ToList();

					//Bob2 should be returned as he has "Bob" in FirstName(boosted) whereas others do not.
					Assert.Equal(2, results.Count);
					Assert.Equal(results[0].Id, "sample/bob2");
					Assert.Equal(results[1].Id, "sample/bob1");
				}
			}
		}
	}
}