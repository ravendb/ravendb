using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;

using Lucene.Net.Search;

using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Tests.Bundles;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2974 : RavenTest
	{
		private class Company
		{
			public string Id { get; set; }

			public string Name { get; set; }

			public CompanyType Type { get; set; }

			public enum CompanyType
			{
				Public,
				Private
			}
		}

		[Fact]
		public void WhenUpdatingByIndex_QueryInputsAreMaintained()
		{
			using (var session = store.OpenSession())
			{
				session.Store(new Company { Name = "Public Company", Type = Company.CompanyType.Public });
				session.Store(new Company { Name = "Private Company", Type = Company.CompanyType.Private });

				session.SaveChanges();
			}

			WaitForIndexing(store);

			var operation = store.DatabaseCommands.UpdateByIndex(new CompanyIndex().IndexName, new IndexQuery()
			{
				Query = "Type:" + Company.CompanyType.Private.ToString("g"),
				TransformerParameters = new Dictionary<string, RavenJToken> { { "QueryInputKey", "value" } }
			}, new[]
            {
                new PatchRequest
                {
                    Type = PatchCommandType.Set,
                    Name = "Type",
                    Value = Company.CompanyType.Public.ToString("g")
                },
            });

			operation.WaitForCompletion();

			Assert.True(SecureIndexesQueryListener.WasFired);

			WaitForIndexing(store);

			using (var session = store.OpenSession())
			{
				var result = session.Query<Company>().ToList();

				Assert.Equal(2, result.Count);
				Assert.True(result.All(x => x.Type == Company.CompanyType.Public));
			}
		}

		public class SecureIndexesQueryListener : AbstractIndexQueryTrigger
		{
			public static bool WasFired;

			public SecureIndexesQueryListener()
			{
				WasFired = false;
			}

			public override Query ProcessQuery(string indexName, Query query, IndexQuery originalQuery)
			{
				if (indexName == new CompanyIndex().IndexName)
				{
					WasFired = true;

					Assert.NotNull(originalQuery.TransformerParameters);
					Assert.Contains("QueryInputKey", originalQuery.TransformerParameters.Keys);
				}
				
				return query;
			}
		}

		private class CompanyIndex : AbstractIndexCreationTask<Company>
		{
			public CompanyIndex()
			{
				Map = companies => from company in companies
								   select new
								   {
									   company.Type
								   };
			}
		}

		private readonly EmbeddableDocumentStore store;

		public RavenDB_2974()
		{
			store = NewDocumentStore(catalog: (new TypeCatalog(typeof(SecureIndexesQueryListener))));
			new CompanyIndex().Execute(store);
		}
	}
}