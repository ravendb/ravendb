namespace Raven.Tests.Silverlight
{
    using System.Collections;
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
}