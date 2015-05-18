// -----------------------------------------------------------------------
//  <copyright file="RavenDB-3472.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class ShardingTransformerTests : RavenTestBase
	{
		[Fact]
		public void TransformerOverShardedLoad_IdIsNotNull()
		{
			using (var server = GetNewServer())
			{
				var shards = new Dictionary<string, IDocumentStore>
             	{
             		{"Asia", new DocumentStore {Url = server.Configuration.ServerUrl, DefaultDatabase = "Asia3"}},
             		{"Middle-East", new DocumentStore {Url = server.Configuration.ServerUrl, DefaultDatabase = "MiddleEast3"}},
             		{"America", new DocumentStore {Url = server.Configuration.ServerUrl, DefaultDatabase = "America3"}},
             	};

				ShardStrategy shardStrategy = new ShardStrategy(shards)
					.ShardingOn<Company>(company => company.Region)
					.ShardingOn<Invoice>(x => x.CompanyId);

				using (IDocumentStore store = new ShardedDocumentStore(shardStrategy))
				{

					store.Initialize();

					new Transformer().Execute(store);

					string americanCompanyId;
					using (IDocumentSession session = store.OpenSession())
					{
						Company asian = new Company { Name = "Company 1", Region = "Asia" };
						session.Store(asian);
						Company middleEastern = new Company { Name = "Company 2", Region = "Middle-East" };
						session.Store(middleEastern);
						Company american = new Company { Name = "Company 3", Region = "America" };
						session.Store(american);

						session.Store(new Invoice { CompanyId = american.Id, Amount = 3, IssuedAt = DateTime.Today.AddDays(-1) });
						session.Store(new Invoice { CompanyId = asian.Id, Amount = 5, IssuedAt = DateTime.Today.AddDays(-1) });
						session.Store(new Invoice { CompanyId = middleEastern.Id, Amount = 12, IssuedAt = DateTime.Today });
						session.SaveChanges();

						americanCompanyId = american.Id;
					}

					using (IDocumentSession session = store.OpenSession())
					{
						var company = session.Load<Transformer, Transformer.Result>(americanCompanyId);

						Assert.Equal(company.Id, americanCompanyId);
					}
				}
			}
		}

		[Fact]
		public void TransformerOverLoad_IdIsNotNull()
		{
			using (IDocumentStore store = NewDocumentStore())
			{

				store.Initialize();

				new Transformer().Execute(store);

				string americanCompanyId;
				using (IDocumentSession session = store.OpenSession())
				{
					Company american = new Company { Name = "Company 3", Region = "America" };
					session.Store(american);

					session.Store(new Invoice { CompanyId = american.Id, Amount = 3, IssuedAt = DateTime.Today.AddDays(-1) });
					session.SaveChanges();

					americanCompanyId = american.Id;
				}

				using (IDocumentSession session = store.OpenSession())
				{
					var company = session.Load<Transformer, Transformer.Result>(americanCompanyId);

					Assert.Equal(company.Id, americanCompanyId);
				}
			}
		}


		public class Transformer : AbstractTransformerCreationTask<Company>
		{
			public class Result
			{
				public string Id { get; set; }
				public string Name { get; set; }
			}

			public Transformer()
			{
				TransformResults = companies => from company in companies
												select new
												{
													Id = company.Id,
													Name = company.Name
												};
			}
		}

		public class Company
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Region { get; set; }
		}

		public class Invoice
		{
			public string Id { get; set; }
			public string CompanyId { get; set; }
			public decimal Amount { get; set; }
			public DateTime IssuedAt { get; set; }
		}
	}

}