// -----------------------------------------------------------------------
//  <copyright file="MultiThreaded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Raven.Abstractions.Data;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Tests;
using Xunit;

namespace Raven.StressTests.Storage.MultiThreaded
{
	public abstract class MultiThreaded : RavenTest
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();
		protected DocumentDatabase DocumentDatabase;
		private readonly ConcurrentQueue<GetDocumentState> getDocumentsState = new ConcurrentQueue<GetDocumentState>();

		private volatile bool run = true;
		
		private Etag lastEtagSeen = Etag.Empty;

		public override void Dispose()
		{
			DocumentDatabase.Dispose();
			base.Dispose();
		}

		protected void SetupDatabaseEsent(bool runInUnreliableMode)
		{
			DocumentDatabase = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = NewDataPath(),
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = runInUnreliableMode,
				DefaultStorageTypeName = typeof(Raven.Storage.Esent.TransactionalStorage).AssemblyQualifiedName,
			});
		}

		protected void SetupDatabase(DocumentDatabase documentDatabase)
		{
			DocumentDatabase = documentDatabase;
		}

		private class GetDocumentState
		{
			private readonly Etag etag;
			private readonly int count;

			public Etag Etag
			{
				get { return etag; }
			}
			public int Count
			{
				get { return count; }
			}

			public GetDocumentState(Etag etag, int count)
			{
				this.etag = etag;
				this.count = count;
			}
		}

		protected void ShouldGetEverything()
		{
			var task = Task.Factory.StartNew(StartGetDocumentOnBackground);
			var count = SetupData();

			var final = Etag.Empty.Setup(UuidType.Documents, 1).IncrementBy(count);
			while (lastEtagSeen != final)
			{
				Thread.Sleep(10);
			}
	
			run = false;
			task.Wait();

			var states = getDocumentsState.ToArray();
			
			Assert.Equal(final, states.Last().Etag);
			Assert.Equal(count, states.Sum(x => x.Count));
		}

		protected abstract int SetupData();

		private void StartGetDocumentOnBackground()
		{
			while (run)
			{
				DocumentDatabase.TransactionalStorage.Batch(accessor =>
				{
					var documents = accessor.Documents.GetDocumentsAfter(lastEtagSeen, 128)
						.Where(x => x != null)
						.Select(doc =>
						{
							DocumentRetriever.EnsureIdInMetadata(doc);
							return doc;
						})
						.ToArray();

					if (documents.Length == 0)
						return;

					lastEtagSeen = documents.Last().Etag;

					log.Debug("Docs: {0}", string.Join(", ", documents.Select(x => x.Key)));

					getDocumentsState.Enqueue(new GetDocumentState(lastEtagSeen, documents.Length));
				});
			}
		}

		[Fact]
		public void WhenUsingEsentOnDisk()
		{
			SetupDatabaseEsent(false);
			ShouldGetEverything();
		}

		[Fact]
		public void WhenUsingEsentInUnreliableMode()
		{
			SetupDatabaseEsent(true);
			ShouldGetEverything();
		}
	}
}