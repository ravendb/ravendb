using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Raven.Abstractions.Commands;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Impl;
using Raven.Json.Linq;
using Raven.Storage.Esent;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class MultiThreaded : IDisposable
	{
		private DocumentDatabase documentDatabase;

		public MultiThreaded()
		{
			IOExtensions.DeleteDirectory(dataDirectory);
			documentDatabase = new DocumentDatabase(new RavenConfiguration()
			                                        {
			                                        	DataDirectory = dataDirectory,
														RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
														RunInMemory = false,
														DefaultStorageTypeName = typeof(TransactionalStorage).AssemblyQualifiedName
			                                        });
		}

		public void Dispose()
		{
			try
			{
				documentDatabase.Dispose();
				IOExtensions.DeleteDirectory(dataDirectory);
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
			}
		}

		public class State
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

			public State(Guid etag, int count)
			{
				this.etag = etag;
				this.count = count;
			}
		}

		volatile bool run = true;
		private const string dataDirectory = "abc";

		[Fact]
		public void ShoudlGetEverything()
		{

			NLog.Logger log = LogManager.GetCurrentClassLogger();
			var state = new ConcurrentQueue<State>();
			Guid lastEtagSeen = Guid.Empty;
			var task = Task.Factory.StartNew(() =>
			                                     	{
			                                     		while (run)
			                                     		{
			                                     			documentDatabase.TransactionalStorage.Batch(accessor =>
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

																												log.Debug("Docs: {0}", string.Join(", ", documents.Select(x=>x.Key)));

			                                     			                                            		state.Enqueue(new State(lastEtagSeen, documents.Length));
			                                     			                                            	});
			                                     		}
			                                     	});


			documentDatabase.Put("Raven/Hilo/users", null, new RavenJObject(), new RavenJObject(), null);
			documentDatabase.Put("Raven/Hilo/posts", null, new RavenJObject(), new RavenJObject(), null);


			documentDatabase.Batch(new[]
			                       {
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "users/1",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/1",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/2",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/3",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/4",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       	new PutCommandData
			                       	{
			                       		Document = new RavenJObject(),
			                       		Etag = null,
			                       		Key = "posts/5",
			                       		Metadata = new RavenJObject(),
			                       		TransactionInformation = null
			                       	},
			                       });


			var final = new Guid("00000000-0000-0100-0000-000000000008");
			while (lastEtagSeen != final)
			{
				Thread.Sleep(10);
			}
	
			run = false;
			
			task.Wait();

			var objects = state.ToArray();
			var last = objects.Last();
			
			Assert.Equal(final, last.Etag);
			Assert.Equal(8, objects.Sum(x=>x.Count));
		}

		
	}
}