// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2812.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.SlowTests.Issues
{
	public class RavenDB_2812 : RavenTest
	{
		public class User
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public List<User> Friends { get; set; }
		}

		public class UsersAndFiendsIndex : AbstractIndexCreationTask<User>
		{
			public override string IndexName
			{
				get
				{
					return "UsersAndFriends";
				}
			}
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition
				{
					Map = @"docs.Users.SelectMany(user => user.Friends, (user, friend) => new {
Name = user.Name
})",
					Stores = new Dictionary<string, FieldStorage>()
					{
						{"Name", FieldStorage.Yes}
					},
					MaxIndexOutputsPerDocument = 16384,
				};
			}
		}

		[Fact]
		public void ShouldProperlyPageResults()
		{
			using (var store = NewDocumentStore())
			{
				new UsersAndFiendsIndex().Execute(store);

				using (var bulk = store.BulkInsert())
				{
					for (int i = 0; i < 50; i++)
					{
						var user = new User()
						{
							Id = "users/" + i,
							Name = "user/" + i,
							Friends = new List<User>(1000)
						};

						var friendsCount = new Random().Next(700, 1000);

						for (int j = 0; j < friendsCount; j++)
						{
							user.Friends.Add(new User()
							{
								Id = "friend/" + i + "/" + j,
								Name = "friend/" + i + "/" + j
							});
						}

						bulk.Store(user);
					}
				}

				WaitForIndexing(store);
				var pagedResults = new List<string>();
				
				const int pageSize = 10;

				using (var session = store.OpenSession())
				{
					for (int page = 0; page < 5; page++)
					{
						var results = session
						.Query<User, UsersAndFiendsIndex>()
						.Select(x=>x.Name)
						.Skip((page * pageSize))
						.Take(pageSize)
						.Distinct()
						.ToList();

						pagedResults.AddRange(results);
					}
				}

				Assert.Equal(50, pagedResults.Distinct().Count());
			}
		}
	}
}