//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
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
			             		{"one", new DocumentStore {Url = "http://localhost:8079"}},
			             		{"two", new DocumentStore {Url = "http://localhost:8078"}},
			             		{"three", new DocumentStore {Url = "http://localhost:8077"}},
			             	};

			var shardStrategy = new ShardStrategy(shards)
				.ShardingOn<Company>()
				.ShardingOn<Invoice>(x => x.CompanyId);

			using (var documentStore = new ShardedDocumentStore(shardStrategy).Initialize())
			{
				new InvoicesAmountByDate().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var asian = new Company { Name = "Company 1" };
					session.Store(asian);
					var middleEastern = new Company { Name = "Company 2" };
					session.Store(middleEastern);
					var american = new Company { Name = "Company 3" };
					session.Store(american);

					session.Store(new Invoice { CompanyId = american.Id, Amount = 3, IssuedAt = DateTime.Today.AddDays(-1) });
					session.Store(new Invoice { CompanyId = asian.Id, Amount = 5, IssuedAt = DateTime.Today.AddDays(-1) });
					session.Store(new Invoice { CompanyId = middleEastern.Id, Amount = 12, IssuedAt = DateTime.Today });
					session.SaveChanges();
				}


				using (var session = documentStore.OpenSession())
				{
					var reduceResults = session.Query<InvoicesAmountByDate.ReduceResult, InvoicesAmountByDate>()
						.ToList();

					foreach (var reduceResult in reduceResults)
					{
						string dateStr = reduceResult.IssuedAt.ToString("MMM dd, yyyy", CultureInfo.InvariantCulture);
						Console.WriteLine("{0}: {1}", dateStr, reduceResult.Amount);
					}
					Console.WriteLine();
				}
			}
		}

	}
}

