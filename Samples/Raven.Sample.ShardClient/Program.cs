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
			
			var shardStrategy = new ShardStrategy
			                    	{
			                    		ShardAccessStrategy = new ParallelShardAccessStrategy(),
										ShardResolutionStrategy = new ShardResolutionByRegion(),
			                    	};

			using (var documentStore = new ShardedDocumentStore(shardStrategy, shards).Initialize())
			using (var session = documentStore.OpenSession())
			{
				//store 3 items in the 3 shards
				session.Store(new Company { Name = "Company 1", Region = "Asia" });
				session.Store(new Company { Name = "Company 2", Region = "Middle East" });
				session.Store(new Company { Name = "Company 3", Region = "America" });
				session.SaveChanges();

				//get all, should automagically retrieve from each shard
				var allCompanies = session.Query<Company>()
					.Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToArray();

				foreach (var company in allCompanies)
					Console.WriteLine(company.Name);
			}
		}
	}
}