using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Raven.Abstractions.Exceptions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class TransactionConcurrencyIssue : RavenTestBase
	{
		private int concurrencyExceptionCount;

		[Fact]
		public void ConsistencyTest()
		{
			var testConfig = new TestConfig(5, 50);
			TestDoc[] documents;

			using(var documentStore = NewRemoteDocumentStore(fiddler:true,runInMemory:false))
			{
				string[] documentIds = RunParallelUpdates(testConfig, documentStore);

				documents = LoadAllDocuments(documentIds, documentStore);

				Trace.WriteLine(string.Format("Number of concurrency exceptions: {0}", concurrencyExceptionCount));
				Trace.WriteLine(string.Format("Number of updates: {0}", documents.Sum(d => d.Version)));
			}

			Assert.Equal(documents.Length, testConfig.NumberOfDocuments);
			Assert.True(documents.All(d => d.Version == testConfig.NumberOfUpdatesPerDocument));
		}

		private TestDoc[] LoadAllDocuments(IEnumerable<string> documentIds, IDocumentStore store)
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Advanced.AllowNonAuthoritativeInformation = false;

				IEnumerable<TestDoc> loadOperation =
					from id in documentIds
					select session.Load<TestDoc>(id);

				return loadOperation.ToArray();
			}
		}

		private string[] RunParallelUpdates(TestConfig testConfig, IDocumentStore store)
		{
			string[] documentIds = CreateTestDocuments(testConfig.NumberOfDocuments, store);

			Parallel.For(0, testConfig.NumberOfUpdatesPerDocument, i => UpdateAllDocumentsInParallel(documentIds, store));

			return documentIds;
		}

		private void UpdateAllDocumentsInParallel(IEnumerable<string> documentIds, IDocumentStore store)
		{
			Parallel.ForEach(documentIds, docId => UpdateDocument(docId, store));
		}

		/// <summary>
		/// Demo update: increment a "Version" property.
		/// </summary>
		private void UpdateDocument(string docId, IDocumentStore store)
		{
			while (true)
				using (IDocumentSession session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					
					try
					{
						using (var tx = new TransactionScope())
						{
							session.Load<TestDoc>(docId).Version++;
							session.SaveChanges();

							tx.Complete();
						}

						return;
					}
					catch (ConcurrencyException)
					{
						Interlocked.Increment(ref concurrencyExceptionCount);
					}
				}
		}

		private string[] CreateTestDocuments(int count, IDocumentStore store)
		{
			TestDoc[] docs = Enumerable.Range(0, count).Select(i => new TestDoc()).ToArray();

			using (IDocumentSession session = store.OpenSession())
			{
				session.Advanced.UseOptimisticConcurrency = true;

				foreach (TestDoc testDoc in docs)
					session.Store(testDoc);

				session.SaveChanges();
			}

			return docs.Select(d => d.Id).ToArray();
		}

		#region Nested type: TestConfig

		private class TestConfig
		{
			private readonly int numberOfDocuments;
			private readonly int numberOfUpdatesPerDocument;

			public TestConfig(int numberOfDocuments, int numberOfUpdatesPerDocument)
			{
				this.numberOfDocuments = numberOfDocuments;
				this.numberOfUpdatesPerDocument = numberOfUpdatesPerDocument;
			}

			public int NumberOfDocuments
			{
				get { return numberOfDocuments; }
			}

			public int NumberOfUpdatesPerDocument
			{
				get { return numberOfUpdatesPerDocument; }
			}
		}

		#endregion

		#region Nested type: TestDoc

		private class TestDoc
		{
			public string Id { get; set; }
			public int Version { get; set; }
		}

		#endregion
	}
}