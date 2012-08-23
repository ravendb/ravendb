using System.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class EnumsCastToInts : RavenTest
	{
		public enum SampleClassKind
		{
			All = 0,
			Nothing
		}

		public class SampleClass
		{
			public string To { get; set; }
			public int Kind { get; set; }
		}


		[Fact]
		public void CanCastInsideWhereClause()
		{
			using (var document = NewDocumentStore())
			{
				const string entityId = "SampleId";

				using (var session = document.OpenSession())
				{
					session.Store(new SampleClass {To = entityId, Kind = (int) SampleClassKind.Nothing});
					session.SaveChanges();
				}

				using (var session = document.OpenSession())
				{
					var kind = SampleClassKind.Nothing;

					var query = from r in session.Query<SampleClass>()
					            	.Customize(x => x.WaitForNonStaleResultsAsOfNow())
					            where r.To == entityId && r.Kind == (int) kind
					            select r;

					Assert.NotNull(query);
					Assert.Equal(1, query.Count());
				}
			}
		}

		[Fact]
		public void CanGetQueryResults()
		{
			using (var document = NewDocumentStore())
			{
				const string entityId = "SampleId";

				using (var session = document.OpenSession())
				{
					session.Store(new SampleClass { To = entityId, Kind = (int)SampleClassKind.Nothing });
					session.SaveChanges();
				}

				using (var session = document.OpenSession())
				{
					var kind = (int)SampleClassKind.Nothing;

					var query = from r in session.Query<SampleClass>()
												 .Customize(x => x.WaitForNonStaleResultsAsOfNow())
								where r.To == entityId && r.Kind == kind
								select r.To;

					Assert.NotNull(query);
					Assert.Equal(entityId, query.First());
				}
			}
		}
	}
}