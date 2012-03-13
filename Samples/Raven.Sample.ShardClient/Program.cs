//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Client;

namespace Raven.Sample.ShardClient
{
	class Program
	{
		static void Main()
		{
			var shards = new Dictionary<string, IDocumentStore>
			             	{
			             		{"Asia", new DocumentStore {Url = "http://localhost:8080"}},
			             		{"Middle East", new DocumentStore {Url = "http://localhost:8081"}},
			             		{"America", new DocumentStore {Url = "http://localhost:8082"}},
			             	};

			var shardStrategy = new ShardStrategy(shards)
				.ShardingOn<Company>(x => x.Region)
				.ShardingOn<Invoice>(x => x.CompanyId);

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					//store 3 items in the 3 shards
					var asian = new Company { Name = "Company 1", Region = "Asia" };
					session.Store(asian);
					var middleEastern = new Company { Name = "Company 2", Region = "Middle East" };
					session.Store(middleEastern);
					var american = new Company { Name = "Company 3", Region = "America" };
					session.Store(american);

					// store 3 invoices
					session.Store(new Invoice { CompanyId = american.Id, Amount = 3 });
					session.Store(new Invoice { CompanyId = asian.Id, Amount = 5 });
					session.Store(new Invoice { CompanyId = middleEastern.Id, Amount = 12 });
					session.SaveChanges();

				}

				using (var session = documentStore.OpenSession())
				{
					session.Query<Company>()
						.Where(x => x.Region == "America")
						.ToList();

					session.Load<Company>("Middle East-companies-1");

					session.Query<Invoice>()
						.Where(x => x.CompanyId == "Asia/companies/1")
						.ToList();
				}
			}
		}
	}
}