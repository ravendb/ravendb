// -----------------------------------------------------------------------
//  <copyright file="MultiThreaded.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Xunit;

namespace Raven.Tests.Storage.MultiThreaded
{
	public abstract class MultiThreaded : IDisposable
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();
		protected DocumentDatabase DocumentDatabase;
		private readonly ConcurrentQueue<GetDocumentState> getDocumentsState = new ConcurrentQueue<GetDocumentState>();

		private volatile bool run = true;
		private static readonly string DataDirectory = typeof(MultiThreaded).FullName + "-Data";
		
		private Guid lastEtagSeen = Guid.Empty;

		public MultiThreaded()
		{
			SafeRun(() => IOExtensions.DeleteDirectory(DataDirectory));
		}

		private void SafeRun(Action action)
		{
			try
			{
				action();
			}
			catch (Exception ex)
			{
				log.ErrorException("An error occurred. See exception for full details.", ex);
				throw;
			}
		}

		public void Dispose()
		{
			SafeRun(() =>
			        	{
			        		DocumentDatabase.Dispose();
			        		IOExtensions.DeleteDirectory(DataDirectory);
			        	});
		}

		protected void SetupDatabase(string defaultStorageTypeName, bool runInMemory)
		{
			DocumentDatabase = new DocumentDatabase(new RavenConfiguration
			                                        {
			                                        	DataDirectory = DataDirectory,
			                                        	RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			                                        	RunInMemory = runInMemory,
														DefaultStorageTypeName = defaultStorageTypeName,
			                                        });
		}

		private class GetDocumentState
		{
			private readonly Guid etag;
			private readonly int count;

			public Guid Etag
			{
				get { return etag; }
			}
			public int Count
			{
				get { return count; }
			}

			public GetDocumentState(Guid etag, int count)
			{
				this.etag = etag;
				this.count = count;
			}
		}

		protected void ShoudlGetEverything()
		{
			var task = Task.Factory.StartNew(StartGetDocumentOnBackground);
			var count = SetupData();

			var final = new Guid("00000000-0000-0100-0000-" + count.ToString("X12"));
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

					lastEtagSeen = documents.Last().Etag.Value;

					log.Debug("Docs: {0}", string.Join(", ", documents.Select(x => x.Key)));

					getDocumentsState.Enqueue(new GetDocumentState(lastEtagSeen, documents.Length));
				});
			}
		}
	}
}