using System.Linq;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Bugs
{
	public class Vlad : ReplicationBase
	{
		public class SampleDoc
		{
			public long Number;
			public string Name;
		}


		[Fact]
		public void TestReplication()
		{
			var store = CreateStore();
			CreateStore();
			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store.OpenSession())
			{
				session.Store(new SampleDoc { Number = 1, Name = "Test 1" }, "SampleDocs/1");
				session.Store(new SampleDoc { Number = 2, Name = "Test 2" }, "SampleDocs/2");
				session.Store(new SampleDoc { Number = 3, Name = "Test 3" }, "SampleDocs/3");
				session.SaveChanges();

				// Force index creation (it must be done BEFORE deleting)
				var selectWarmup = session.Query<SampleDoc>().Select(x => new { x.Name, x.Number }).ToList();
			}

			using (var session = store.OpenSession())
			{
				var doc = session.Load<SampleDoc>("SampleDocs/2");
				session.Delete(doc);
				session.SaveChanges();
			}

			using (var session = store.OpenSession())
			{
				var selectGood = session.Query<SampleDoc>().ToList();
				Assert.Equal(2, selectGood.Count);

				var selectBad = session.Query<SampleDoc>().Customize(x => x.WaitForNonStaleResults())
					.Select(x => new { x.Name, x.Number }).ToList();
				var count = session.Query<SampleDoc>().Customize(x => x.WaitForNonStaleResultsAsOfLastWrite()).Count();

				Assert.Equal(2, selectBad.Count);
				Assert.Equal(2, count);
			}
		}
	}
}