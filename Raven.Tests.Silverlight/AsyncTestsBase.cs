namespace Raven.Tests.Silverlight
{
	using System;
	using System.Threading.Tasks;
	using Raven.Client.Document;
	using Raven.Client.Extensions;
	using Microsoft.Silverlight.Testing;

	public abstract class AsyncTestsBase : SilverlightTest
	{
		const int Port = 8080;
		const string Url = "http://localhost:";

		protected static string GenerateNewDatabaseName()
		{
			return Guid.NewGuid().ToString();
		}

		protected DocumentStore EnqueueEnsuringNewDatabase(string dbname)
		{
			var documentStore = new DocumentStore { Url = Url + Port };
			documentStore.Initialize();
			var ensure = documentStore.AsyncDatabaseCommands.EnsureDatabaseExistsAsync(dbname);
			EnqueueTaskCompleted(ensure);

			return documentStore;
		}

		protected void EnqueueTaskCompleted(Task task)
		{
			EnqueueConditional(() => task.IsCompleted || task.IsFaulted);
		}
	}
}