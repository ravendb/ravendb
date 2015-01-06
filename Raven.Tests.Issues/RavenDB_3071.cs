using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Shard;
using Raven.Server;
using Raven.Tests.Helpers;
using Xunit;
using System.Threading.Tasks;

namespace Raven.Tests.Issues
{
    public class RavenDb_3071 : RavenTestBase
    {
        public class Foo
        {
            public string Name { get; set; }
        }

        [Export(typeof(AbstractIndexCreationTask<Foo>))]
        public class Index1 : AbstractIndexCreationTask<Foo>
        {
            public Index1()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.Name + "A"
                              };
            }
        }

        [Export(typeof(AbstractIndexCreationTask<Foo>))]
        public class Index2 : AbstractIndexCreationTask<Foo>
        {
            public Index2()
            {
                Map = foos => from foo in foos
                              select new
                              {
                                  Name = foo.Name + "C"
                              };
            }
        }


        private new readonly RavenDbServer[] servers;
        private readonly ShardedDocumentStore documentStore;

        public RavenDb_3071()
        {
            servers = new[]
			{
				GetNewServer(8079),
				GetNewServer(8078),
				GetNewServer(8077),
			};

            documentStore = new ShardedDocumentStore(new ShardStrategy(new Dictionary<string, IDocumentStore>
			{
				{"1", CreateDocumentStore(8079)},
				{"2", CreateDocumentStore(8078)},
				{"3", CreateDocumentStore(8077)}
			}));
            documentStore.Initialize();
        }


        private static IDocumentStore CreateDocumentStore(int port)
        {
            return new DocumentStore
            {
                Url = string.Format("http://localhost:{0}/", port),
                DefaultDatabase = "Test",
                
                Conventions =
                {
                    FailoverBehavior = FailoverBehavior.FailImmediately
                }
            };
        }

        public class IndexManager
        {
            [ImportMany]
#pragma warning disable 649
            private IEnumerable<Lazy<AbstractIndexCreationTask<Foo>>> _indexes;
#pragma warning restore 649

            public IEnumerable<AbstractIndexCreationTask<Foo>> Indexes
            {
                get { return _indexes.Select(x => x.Value).ToList(); }
            }

        }

        [Fact]
        public async Task CreateMultipleIndexesOnMultipleShardsAsync()
        {
            try
            {
                var indexManager = new IndexManager();
                var container = new CompositionContainer();
                container.ComposeParts(indexManager, new Index1(), new Index2());
                await IndexCreation.CreateIndexesAsync(container, documentStore);
                foreach (var shard in documentStore.ShardStrategy.Shards.Values)
                {
                    var indexInfo = shard.DatabaseCommands.GetStatistics().Indexes;
                    Assert.True(indexInfo.Any(index => index.Name.Equals("Index1")));
                    Assert.True(indexInfo.Any(index => index.Name.Equals("Index2")));
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        public override void Dispose()
        {
            documentStore.Dispose();
            foreach (var server in servers)
            {
                server.Dispose();
            }
            base.Dispose();
        }
    }
}
