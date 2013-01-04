using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.MailingList.spokeypokey
{
	public class spokeypokey : RavenTest
	{
		public class BarnIndex : AbstractIndexCreationTask<Barn, Barn>
		{
			public BarnIndex()
			{
				Map =
					barnlist =>
					from barn in barnlist
					from household in barn.Households
					from member in household.Members
					select new
					{
						barn.InternalId,
						barn.Name,
						HouseholdId = household.InternalId,
						MemberId = member.InternalId,
						MembersName = member.Name
					};

			}
		}

		[Fact]
		public void Can_use_barn_index2()
		{
			using (var docStore = NewDocumentStore())
			{
				docStore.Conventions.FindPropertyNameForIndex = (indexedType, indexedName, path, prop) =>
				{
					var result = path + prop;
					switch (result)
					{
						case "Households,Members,Name":
							return "MembersName";
						default:
							return result;
					}
				};
				new BarnIndex().Execute(docStore);

				var barn1 = new Barn
				            	{
				            		Name = "Barn1",
				            		Households = new List<Household>
				            		             	{
				            		             		new Household
				            		             			{
				            		             				Address = "123 Main St",
				            		             				Members = new List<Member> {new Member {Name = "Joe"},},
				            		             			},
				            		             	}
				            	};
				using (var session = docStore.OpenSession())
				{
					session.Store(barn1);
					session.SaveChanges();
				}

				using (var session = docStore.OpenSession())
				{
					RavenQueryStatistics statistics;

					// Query using dynamic index
					var result1 = from b in session.Query<Barn>()
					              	.Customize(x => x.WaitForNonStaleResults())
					              	.Statistics(out statistics)
					              where b.Households.Any(h => h.Members.Any(m => m.Name == "Joe"))
					              select b;
					var result1List = result1.ToList();
					Assert.Equal(1, result1List.Count());

					// Query using BarnIndex
					var result2 = from b in session.Query<Barn, BarnIndex>()
									.Customize(x => x.WaitForNonStaleResults())
									.Statistics(out statistics)
					              where b.Name == "Barn1"
					              select b;
					var result2List = result2.ToList();
					var indexName2 = statistics.IndexName;
					Assert.Equal("BarnIndex", indexName2);
					Assert.Equal(1, result2List.Count());

					// Query using BarnIndex
					var result3 = from b in session.Query<Barn, BarnIndex>()
					              	.Customize(x => x.WaitForNonStaleResults())
					              	.Statistics(out statistics)
								  where b.Households.Any(h => h.Members.Any(m => m.Name == "Joe"))
					              select b;
					var result3List = result3.ToList();
					var indexName3 = statistics.IndexName;
					Assert.Equal("BarnIndex", indexName3);

					// Test fails here!!
					Assert.Equal(1, result3List.Count());
				}
			}
		}

		public class Barn
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
			public IList<Household> Households { get; set; }
		}

		public class Household
		{
			public string InternalId { get; set; }
			public string Address { get; set; }
			public IList<Member> Members { get; set; }
		}

		public class Member
		{
			public string InternalId { get; set; }
			public string Name { get; set; }
		}


	}
}
