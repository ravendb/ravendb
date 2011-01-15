namespace Raven.Tests.Silverlight
{
	using System;
	using System.Collections;
	using System.Collections.Generic;
	using System.Reflection;
	using System.Threading.Tasks;
	using Client.Document;
	using Client.Extensions;
	using Document;
	using Microsoft.Silverlight.Testing;
	using Microsoft.VisualStudio.TestTools.UnitTesting;
	using Assert = Xunit.Assert;

	public class LinqQueryTests : AsyncTestsBase
	{
		[Asynchronous]
		[TestMethod]
		public void Can_insert_async_and_multi_get_async()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = EnqueueEnsuringNewDatabase(dbname);

			EnqueueCallback(() =>
			                	{
			                		var entity1 = new Company {Name = "Async Company #1"};
			                		var entity2 = new Company {Name = "Async Company #2"};
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
			                		                			var task =
			                		                				session_for_loading.MultiLoadAsync<Company>(new[] {entity1.Id, entity2.Id});
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
	}

	[TestClass]
	public class MyTestBase : SilverlightTest
	{
		const int Port = 8080;
		const string Url = "http://localhost:";

		protected static string GenerateNewDatabaseName()
		{
			return Guid.NewGuid().ToString();
		}

		public void ExecuteTest(MethodInfo test)
		{
			var tasks = (IEnumerable<Task>)test.Invoke(this, new object[] { });
			var enumerator = tasks.GetEnumerator();
			ExecuteTestStep(enumerator);
		}

		void ExecuteTestStep(IEnumerator<Task> enumerator)
		{
			bool moveNextSucceeded = false;
			try
			{
				moveNextSucceeded = enumerator.MoveNext();
			}
			catch (Exception ex)
			{
				EnqueueTestComplete();
				return;
			}

			if (moveNextSucceeded)
			{
				try
				{
					var next = enumerator.Current;
					EnqueueConditional(() => next.IsCompleted || next.IsFaulted);
					EnqueueCallback( ()=> ExecuteTestStep(enumerator));
				}
				catch (Exception ex)
				{
					EnqueueTestComplete();
					return;
				}
			}
			else EnqueueTestComplete(); ;
		}

        [TestMethod]
        [Asynchronous]
		public IEnumerable<Task> TestStuff()
		{
			var dbname = GenerateNewDatabaseName();
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			yield return documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);

			var entity1 = new Company {Name = "Async Company #1"};
			var entity2 = new Company {Name = "Async Company #2"};
			using (var session_for_storing = documentStore.OpenAsyncSession(dbname))
			{
				session_for_storing.Store(entity1);
				session_for_storing.Store(entity2);
				yield return session_for_storing.SaveChangesAsync();
			}

			using (var session_for_loading = documentStore.OpenAsyncSession(dbname))
			{
				var task = session_for_loading.MultiLoadAsync<Company>(new[] {entity1.Id, entity2.Id});
				yield return task;

				Assert.Equal(entity1.Name, task.Result[0].Name);
				Assert.Equal(entity2.Name, task.Result[1].Name);

				EnqueueTestComplete();
			}
		}
	}
}