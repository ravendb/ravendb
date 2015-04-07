// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3298.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;

using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3298 : ReplicationBase
	{
		[Fact]
		public void CanUseTransformerWithShard()
		{
			var company1 = new Company { Name = "Company1" };
			var company2 = new Company { Name = "Company2" };

			var store1 = CreateStore();
			var store2 = CreateStore();

			//get them in simple single threaded sequence for this test
			var shardStrategy = new ShardStrategy(new Dictionary<string, IDocumentStore>
			                                      {
				                                      {"shard1", store1},
													  {"shard2", store2}
			                                      })
								{
									ShardAccessStrategy = new SequentialShardAccessStrategy()
								};

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					documentStore.ExecuteTransformer(new Company_EntityTransformer());

					session.Store(company1);
					session.Store(company2);
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var allCompanies = session
						.Query<Company>()
						.Customize(x => x.WaitForNonStaleResults())
						.TransformWith<Company_EntityTransformer, Company>()
						.ToList();

					Assert.NotNull(allCompanies);
					Assert.Equal(company1.Name.ToLowerInvariant(), allCompanies[0].Name);
					Assert.Equal(company2.Name.ToLowerInvariant(), allCompanies[1].Name);
				}

				using (var session = documentStore.OpenSession())
				{
					var transformerName = new Company_EntityTransformer().TransformerName;

					var allCompanies = session
						.Advanced
						.DocumentQuery<Company>()
						.WaitForNonStaleResults()
						.SetResultTransformer(transformerName)
						.ToList();

					Assert.NotNull(allCompanies);
					Assert.Equal(company1.Name.ToLowerInvariant(), allCompanies[0].Name);
					Assert.Equal(company2.Name.ToLowerInvariant(), allCompanies[1].Name);
				}
			}
		}

		private class Company_EntityTransformer : AbstractTransformerCreationTask<Company>
		{
			public Company_EntityTransformer()
			{
				TransformResults = companies => from company in companies
												select new
												{
													company.Id,
													Name = company.Name.ToLowerInvariant()
												};
			}
		}
	}
}