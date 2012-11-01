using System;
using System.Linq;
using System.Reflection;
using FizzWare.NBuilder;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB542 : RavenTest
	{
		[Fact]
		public void MapWithMinValueComparison()
		{
			using (var store = NewDocumentStore())
			{
				new OrganizationIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					var orgs = Builder<Organization>.CreateListOfSize(10)
						.All()
						.Random(10)
						.Build();

					foreach (var org in orgs)
						session.Store(org);
					session.SaveChanges();

					store.DatabaseCommands.Patch("organizations/1",
					                             new[]
					                             {
						                             new PatchRequest
						                             {
							                             Type = PatchCommandType.Set,
							                             Name = "DateApproved",
							                             Value = "2012-09-07T09:41:42.9893269"
						                             }
					                             });

				
					WaitForIndexing(store);

					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				}
			}
		}

		class Organization
		{
			public int Id { get; set; }
			public string Name { get; set; }
			public DateTime DateApproved { get; set; }
		}

		class OrganizationIndex : AbstractIndexCreationTask<Organization, OrganizationIndex.Result>
		{
			public class Result
			{
				public bool IsApproved { get; set; }
			}

			public OrganizationIndex()
			{
				Map = orgs => orgs.Select(org => new
				{
					org.Name,
					IsApproved = org.DateApproved == DateTime.MinValue
				});
			}
		}
	}
}