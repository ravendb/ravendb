using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Logging;
using Raven.Client;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	/// <remarks>
	/// issue opened as consequence of conversion in mailing list --> https://groups.google.com/forum/#!topic/ravendb/o3PRd8M5b3A
	/// </remarks>
	public class RavenDB_1493 : RavenTestBase
	{
		private int concurrencyExceptionCount;
		private int concurrentUpdatesCount;
		private static readonly ILog log = LogManager.GetCurrentClassLogger();

		private ConcurrentDictionary<string, ConcurrentBag<string>> concurrentUpdateLog;

		[Fact]
		public void ConsistencyTest()
		{
			concurrentUpdateLog = new ConcurrentDictionary<string, ConcurrentBag<string>>();
			var testConfig = new TestConfig(2, 55);
			concurrentUpdatesCount = 0;
			TestDoc[] documents;
			var expectedVersionLog = String.Empty;

			for (int i = 1; i <= testConfig.NumberOfUpdatesPerDocument; i++)
				expectedVersionLog += (i + ";");

			using(var documentStore = NewRemoteDocumentStore(requestedStorage:"esent",runInMemory:false))
			{
                if(documentStore.DatabaseCommands.GetStatistics().SupportsDtc == false)
                    return;

				documentStore.Conventions.ShouldCacheRequest = url => false;
				documentStore.Conventions.ShouldAggressiveCacheTrackChanges = false;
				documentStore.Conventions.ShouldSaveChangesForceAggressiveCacheCheck = false;
				documentStore.JsonRequestFactory.DisableAllCaching();

				var documentIds = RunParallelUpdates(testConfig, documentStore);

				documents = LoadAllDocuments(documentIds, documentStore);

				Trace.WriteLine(string.Format("Number of concurrency exceptions: {0}", concurrencyExceptionCount));
				Trace.WriteLine(string.Format("Number of updates: {0}", documents.Sum(d => d.Version)));
			}

			Assert.Equal(documents.Length, testConfig.NumberOfDocuments);
			Assert.Equal(testConfig.NumberOfDocuments * testConfig.NumberOfUpdatesPerDocument, concurrentUpdatesCount);


			foreach (var docKvp in concurrentUpdateLog)
			{
				var docVersionLogSteps = docKvp.Value.OrderBy(row => row.Length).ToList();
				expectedVersionLog = String.Empty;
				Trace.WriteLine("doc key : " + docKvp.Key);
				//validate that steps for each document were submitted only once
				for (int i = 1; i <= testConfig.NumberOfUpdatesPerDocument; i++)
				{
					expectedVersionLog += (i + ";");
					Trace.WriteLine(expectedVersionLog);
					Assert.True(docVersionLogSteps.Count(versionLog => versionLog.Equals(expectedVersionLog)) == 1); //must occur exactly once
				}
			}
			foreach (var document in documents)
				Assert.Equal(document.VersionLog, expectedVersionLog);

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

		private IEnumerable<string> RunParallelUpdates(TestConfig testConfig, IDocumentStore store)
		{
			var documentIds = CreateTestDocuments(testConfig.NumberOfDocuments, store);
			WaitForIndexing(store);
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
			{

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					//session.Advanced.AllowNonAuthoritativeInformation = false;
					try
					{
						string currentVersionLog;
						using (var tx = new TransactionScope())
						{
							var currentDoc = session.Load<TestDoc>(docId);

							log.Info("[test] UpdateDocument() session.Load<T>(docId = {0}) Version = {1}", docId, currentDoc.Version);
							currentDoc.Version++;

							currentDoc.VersionLog += (currentDoc.Version + ";");
							currentVersionLog = currentDoc.VersionLog;

							session.SaveChanges();
							log.Info("[test] UpdateDocument() docId = {0},Version = {1} --> SaveChanges()", docId, currentDoc.Version - 1);
							tx.Complete();
						}

						Interlocked.Increment(ref concurrentUpdatesCount);

						//record only successfull tx results
						concurrentUpdateLog.AddOrUpdate(docId, new ConcurrentBag<string>(new[] {currentVersionLog}),
							(id, versionLogCollection) =>
							{
								versionLogCollection.Add(currentVersionLog);
								return versionLogCollection;
							});
						return;
					}
					catch (TransactionAbortedException)
					{
						Interlocked.Increment(ref concurrencyExceptionCount);
					}
					catch (ConcurrencyException)
					{
						Interlocked.Increment(ref concurrencyExceptionCount);
					}
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
			public string VersionLog { get; set; }
			public int Version { get; set; }

			public TestDoc()
			{
				Version = 0;
				VersionLog = String.Empty;
			}
		}

		#endregion
	}
}