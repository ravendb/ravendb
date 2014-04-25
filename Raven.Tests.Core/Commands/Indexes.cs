// -----------------------------------------------------------------------
//  <copyright file="Crud.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Xunit;
using Raven.Tests.Core.Utils.Entities;

namespace Raven.Tests.Core.Commands
{
	public class Indexes : RavenCoreTestBase
	{
		[Fact]
		public async Task CanPutUpdateAndDeleteMapIndex()
		{
			using (var store = GetDocumentStore())
			{
				const string usersByname = "users/byName";

				await store.AsyncDatabaseCommands.PutAsync(
					"users/1",
					null,
					RavenJObject.FromObject(new User
					{
						Name = "testname"

					}),
					new RavenJObject());

				await store.AsyncDatabaseCommands.PutIndexAsync(usersByname, new IndexDefinition()
				{
					Map = "from user in docs.Users select new { user.Name }"
				}, false);

				var index = await store.AsyncDatabaseCommands.GetIndexAsync(usersByname);
				Assert.Equal(usersByname, index.Name);

				var indexes = await store.AsyncDatabaseCommands.GetIndexesAsync(0, 5);
				Assert.Equal(1, indexes.Length);

				await store.AsyncDatabaseCommands.DeleteIndexAsync(usersByname);
				Assert.Null(await store.AsyncDatabaseCommands.GetIndexAsync(usersByname));
			}
		}

		[Fact]
		public async Task CanPutUpdateAndDeleteTransformer()
		{
			using (var store = GetDocumentStore())
			{
				const string usersSelectNames = "users/selectName";

				await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
				{
					Name = usersSelectNames,
					TransformResults = "from user in results select new { user.FirstName, user.LastName }"
				});

				await store.AsyncDatabaseCommands.PutTransformerAsync(usersSelectNames, new TransformerDefinition()
				{
					Name = usersSelectNames,
					TransformResults = "from user in results select new { Name = user.Name }"
				});

				var transformer = await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames);
				Assert.Equal("from user in results select new { Name = user.Name }", transformer.TransformResults);

				var transformers = await store.AsyncDatabaseCommands.GetTransformersAsync(0, 5);
				Assert.Equal(1, transformers.Length);

				await store.AsyncDatabaseCommands.DeleteTransformerAsync(usersSelectNames);
				Assert.Null(await store.AsyncDatabaseCommands.GetTransformerAsync(usersSelectNames));
			}
		}

		[Fact]
		public void CanGetTermsForIndex()
		{
			using (var store = GetDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 15; i++)
					{
						s.Store(new User { Name = "user" + i.ToString("000") });
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});

				WaitForIndexing(store);

				var terms = store.DatabaseCommands.GetTerms("test", "Name", null, 10)
					.OrderBy(x => x)
					.ToArray();

				Assert.Equal(10, terms.Length);

				for (int i = 0; i < 10; i++)
				{
					Assert.Equal("user" + i.ToString("000"), terms.ElementAt(i));
				}
			}
		}

		[Fact]
		public void CanGetTermsForIndex_WithPaging()
		{
			using (var store = GetDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 15; i++)
					{
						s.Store(new User { Name = "user" + i.ToString("000") });
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});

				WaitForIndexing(store);

				var terms = store.DatabaseCommands.GetTerms("test", "Name", "user009", 10)
					.OrderBy(x => x)
					.ToArray();

				Assert.Equal(5, terms.Count());

				for (int i = 10; i < 15; i++)
				{
					Assert.Equal("user" + i.ToString("000"), terms.ElementAt(i - 10));
				}
			}
		}

		[Fact]
		public void CanQueryForMetadataAndIndexEntriesOnly()
		{
			using (var store = GetDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 5; i++)
					{
						s.Store(new User { Name = "user" + i });
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }",
													SortOptions = new Dictionary<string, SortOptions>()
													{
														{"Name", SortOptions.String}
													}
												});

				WaitForIndexing(store);

				var metadataOnly = store.DatabaseCommands.Query("test", new IndexQuery(), null, metadataOnly: true).Results;

				Assert.True(metadataOnly.TrueForAll(x => x.Keys.Count == 1 && x.Keys.First() == Constants.Metadata));

				var entriesOnly = store.DatabaseCommands.Query("test", new IndexQuery()
				{
					SortedFields = new[] { new SortedField("Name") }
				}, null, indexEntriesOnly: true).Results;

				for (int i = 0; i < 5; i++)
				{
					Assert.Equal(2, entriesOnly[i].Keys.Count);

					Assert.Equal("user" + i, entriesOnly[i].Value<string>("Name"));
					Assert.Equal("users/" + (i + 1), entriesOnly[i].Value<string>(Constants.DocumentIdFieldName));
				}
			}
		}
	}
}
