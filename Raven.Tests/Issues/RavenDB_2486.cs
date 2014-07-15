using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Lucene.Net.Analysis;
using Raven.Abstractions.Exceptions;
using Raven.Client.Indexes;
using Raven.Tests.Bugs.MultiMap;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_2486 : RavenTestBase
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
								  Name = foo.Name + "B"
							  };
				Analyze(x => x.Name,"NotExistingAnalyzerClassName");
			}
		}

		[Export(typeof(AbstractIndexCreationTask<Foo>))]
		public class Index3 : AbstractIndexCreationTask<Foo>
		{
			public Index3()
			{
				Map = foos => from foo in foos
							  select new
							  {
								  Name = foo.Name + "C"
							  };
			}
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
		public void Multiple_indexes_created_with_not_existing_analyzer_should_skip_only_the_invalid_index()
		{			
			using (var store = NewRemoteDocumentStore())
			{
				var indexManager = new IndexManager();
				var container = new CompositionContainer();
				container.ComposeParts(indexManager,new Index1(),new Index2(),new Index3());

				try
				{
					IndexCreation.CreateIndexes(container, store);
				}
				catch (AggregateException e)
				{
					Assert.Contains("Index2",e.InnerExceptions.First().Message);
				}

				var indexInfo = store.DatabaseCommands.GetStatistics().Indexes;
				Assert.Equal(3, indexInfo.Length); //the third is Raven/DocumentEntityByName
				Assert.True(indexInfo.Any(index => index.PublicName.Equals("Index1")));
				Assert.True(indexInfo.Any(index => index.PublicName.Equals("Index3")));
			}
		}

		[Fact]
		public async Task Multiple_indexes_created_withAsync_AndWith_not_existing_analyzer_should_skip_only_the_invalid_index()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var indexManager = new IndexManager();
				var container = new CompositionContainer();
				container.ComposeParts(indexManager, new Index1(), new Index2(), new Index3());

				try
				{
					await IndexCreation.CreateIndexesAsync(container, store);
				}
				catch (AggregateException e)
				{
					Assert.Contains("Index2", e.InnerExceptions.First().Message);
				}

				var indexInfo = store.DatabaseCommands.GetStatistics().Indexes;
				Assert.Equal(3, indexInfo.Length); //the third is Raven/DocumentEntityByName
				Assert.True(indexInfo.Any(index => index.PublicName.Equals("Index1")));
				Assert.True(indexInfo.Any(index => index.PublicName.Equals("Index3")));
			}
		}
	}
}
