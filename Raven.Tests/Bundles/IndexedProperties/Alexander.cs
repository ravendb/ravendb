using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bundles.IndexedProperties
{
	public class Alexander : RavenTest
	{
		public class IndexedPropertyTarget
		{
			public string Id { get; set; }
			public decimal? LastAmount { get; set; }
			public DateTime LastRefreshedOn { get; set; }
		}

		public class IndexedPropertySource
		{
			public string Id { get; set; }
			public string TargetId { get; set; }
			public decimal? Amount { get; set; }
		}

		public class IndexedPropertySource_LastAmount : AbstractIndexCreationTask<IndexedPropertySource, IndexedPropertySource_LastAmount.Result>
		{
			public class Result
			{
				public string TargetId { get; set; }
				public decimal? LastAmount { get; set; }
				public DateTime LastRefreshedOn { get; set; }
			}

			public IndexedPropertySource_LastAmount()
			{
				Map = sources => sources.Select(a => new
				{
					TargetId = a.TargetId,
					LastAmount = a.Amount,
					LastRefreshedOn = MetadataFor(a).Value<DateTime>("Last-Modified")
				});
				Reduce = results => results.GroupBy(a => a.TargetId).Select(a => new
				{
					last = a.OrderByDescending(b => b.LastRefreshedOn).First()
				}).Select(a => new
				{
					a.last.TargetId,
					a.last.LastAmount,
					a.last.LastRefreshedOn
				});
			}
		}

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.Settings.Add("Raven/ActiveBundles", "IndexedProperties");
		}

		[Fact]
		public void IndexedPropertiesUpdate()
		{
			using (var docStore = NewDocumentStore())
			{
				var index = new IndexedPropertySource_LastAmount();
				index.Execute(docStore);

				docStore.DatabaseCommands.Put("Raven/IndexedProperties/" + index.IndexName,
										null,
										RavenJObject.FromObject(new IndexedPropertiesSetupDoc
										{
											DocumentKey = "TargetId",
											FieldNameMappings =
                                                      {
                                                          {"LastAmount", "LastAmount"},
                                                          {"LastRefreshedOn", "LastRefreshedOn"}
                                                      }
										}),
										new RavenJObject());

				using (var session = docStore.OpenSession())
				{
					session.Store(new IndexedPropertyTarget { Id = "Target/1" });
					session.SaveChanges();
				}
				using (var session = docStore.OpenSession())
				{
					session.Store(new IndexedPropertySource()
					{
						Id = "Source/1",
						TargetId = "Target/1",
						Amount = 100.00m,
					});
					session.SaveChanges();
				}

				WaitForIndexing(docStore);

				using (var session = docStore.OpenSession())
				{
					var target = session.Load<IndexedPropertyTarget>("Target/1");
					//this works as expected
					Assert.Equal(target.LastAmount, 100.00m);
					session.Store(new IndexedPropertySource()
					{
						Id = "Source/2",
						TargetId = "Target/1",
						Amount = null,
					});
					session.SaveChanges();
				}
				WaitForIndexing(docStore);
				using (var session = docStore.OpenSession())
				{
					//this throws deserialization exception.
					//If you look at the document in studio it has "NULL_VALUE" instead of null
					var target = session.Load<IndexedPropertyTarget>("Target/1");
				}

			}
		}
	}
}