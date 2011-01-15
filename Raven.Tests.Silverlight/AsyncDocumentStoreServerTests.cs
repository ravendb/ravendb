namespace Raven.Tests.Silverlight
{
	using System.Collections.Generic;
	using System.Linq;
	using Database.Data;
	using Database.Indexing;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Document;
	using Assert = Xunit.Assert;

	[TestClass]
	public class AsyncDocumentStoreServerTests : AsyncTestsBase
	{
		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_multi_get_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var entity1 = new Company { Name = "Async Company #1" };
				var entity2 = new Company { Name = "Async Company #2" };
				using (var session_for_storing = documentStore.OpenAsyncSession(dbname))
				{
					session_for_storing.Store(entity1);
					session_for_storing.Store(entity2);
					EnqueueTaskCompleted(session_for_storing.SaveChangesAsync());
				}

				EnqueueCallback(() =>
				{
					using (var session_for_loading = documentStore.OpenAsyncSession(dbname))
					{
						var task = session_for_loading.MultiLoadAsync<Company>(new[] { entity1.Id, entity2.Id });
						EnqueueTaskCompleted(task);
						EnqueueCallback(() =>
						{
							Assert.Equal(entity1.Name, task.Result[0].Name);
							Assert.Equal(entity2.Name, task.Result[1].Name);
							EnqueueTestComplete();
						});
					}
				});
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_load_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var entity = new Company { Name = "Async Company #1" };
				using (var session_for_storing = documentStore.OpenAsyncSession(dbname))
				{
					session_for_storing.Store(entity);
					EnqueueTaskCompleted(session_for_storing.SaveChangesAsync());
				}

				EnqueueCallback(() =>
				{
					using (var session_for_loading = documentStore.OpenAsyncSession(dbname))
					{
						var task = session_for_loading.LoadAsync<Company>(entity.Id);
						EnqueueTaskCompleted(task);
						EnqueueCallback(() =>
											{
												Assert.Equal(entity.Name, task.Result.Name);
												EnqueueTestComplete();
											});
					}

				});
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_delete_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					EnqueueTaskCompleted(session.SaveChangesAsync());

					EnqueueCallback(() =>
										{
											using (var for_loading = documentStore.OpenAsyncSession(dbname))
											{
												var loading = for_loading.LoadAsync<Company>(entity.Id);
												EnqueueTaskCompleted(loading);
												EnqueueCallback(() => Assert.NotNull(loading.Result));
											}
										});

					EnqueueCallback(() =>
					{
						using (var for_deleting = documentStore.OpenAsyncSession(dbname))
						{
							var loading = for_deleting.LoadAsync<Company>(entity.Id);
							EnqueueTaskCompleted(loading);
							EnqueueCallback(() =>
							{
								for_deleting.Delete(loading.Result);
								EnqueueTaskCompleted(for_deleting.SaveChangesAsync());

								EnqueueCallback(() =>
											{
												using (var for_verifying = documentStore.OpenAsyncSession(dbname))
												{
													var verification = for_verifying.LoadAsync<Company>(entity.Id);
													EnqueueTaskCompleted(verification);
													EnqueueCallback(() => Assert.Null(verification.Result));
													EnqueueTestComplete();
												}
											});
							});
						}
					});
				}
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_get_index_names_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexNamesAsync(0, 25);
				EnqueueTaskCompleted(task);
				EnqueueCallback(() =>
									{
										Assert.Equal(new[] { "Raven/DocumentsByEntityName" }, task.Result);
										EnqueueTestComplete();
									});
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_put_an_index_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("Test", new IndexDefinition
				{
					Map = "from doc in docs.Companies select new { doc.Name }"
				}, true);
				EnqueueTaskCompleted(task);

				EnqueueCallback(() =>
				{
					var verification = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).GetIndexNamesAsync(0, 25);
					EnqueueTaskCompleted(verification);
					EnqueueCallback(() =>
					{
						Assert.Contains(task.Result, "Test");
						EnqueueTestComplete();
					});
				});
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_query_by_index()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			{
				var entity = new Company { Name = "Async Company #1", Id = "companies/1" };
				using (var session = documentStore.OpenAsyncSession(dbname))
				{
					session.Store(entity);
					EnqueueTaskCompleted(session.SaveChangesAsync());
				}

				EnqueueCallback(() =>
				{
					var task = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).PutIndexAsync("Test", new IndexDefinition
					{
						Map =
							"from doc in docs.Companies select new { doc.Name }"
					}, true);
					EnqueueTaskCompleted(task);

					EnqueueCallback(() =>
					{
						//TODO: need to wait until the indexing is done. BAD CODE!!!!
						EnqueueDelay(500);

						EnqueueCallback(() =>
						{
							var query = documentStore.AsyncDatabaseCommands.ForDatabase(dbname).QueryAsync("Test", new IndexQuery(), null);
							EnqueueTaskCompleted(query);
							EnqueueCallback(() =>
							{
								var r = query.Result;
								Assert.NotEqual(0, r.TotalResults);
								EnqueueTestComplete();
							});
						});
					});


				});
			});
		}

		[Asynchronous]
		[TestMethod]
		public void Can_project_value_from_collection()
		{
			var dbname = GenerateNewDatabaseName();
			var store = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(()=>
			{
				using (var session = store.OpenAsyncSession(dbname))
			    {
					session.Store(new Company
					{
						Name = "Project Value Company",
						Contacts = new List<Contact>
                        {
                            new Contact { Surname = "Abbot" },
                            new Contact { Surname = "Costello" }
                        }
					});
					EnqueueTaskCompleted(session.SaveChangesAsync());

					//TODO: need to wait until the indexing is done. BAD CODE!!!!
					EnqueueDelay(500);

					EnqueueCallback(()=>
					{
						var query = store.AsyncDatabaseCommands.ForDatabase(dbname).QueryAsync("dynamic",
														   new IndexQuery
														   {
															   FieldsToFetch = new[] { "Contacts,Surname" }
														   },
														   new string[0]);   
						EnqueueTaskCompleted(query);
						EnqueueCallback(()=>
						{
							Assert.Equal(2, query.Result.Results[0]["Contacts"].Count());
							Assert.Equal("Abbot", query.Result.Results[0]["Contacts"][0].Value<string>("Surname"));
							Assert.Equal("Costello", query.Result.Results[0]["Contacts"][1].Value<string>("Surname"));
							EnqueueTestComplete();
						});
					});		
						
			    }
			});
		
		}
	}
}