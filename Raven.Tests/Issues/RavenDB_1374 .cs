// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1374 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1374 : RavenTest
	{
		public class Item
		{
			public string Foo { get; set; }
			public int Bar { get; set; }
			public List<string> Bazes { get; set; }
		}

		public class MaxNumberOfOutputs_MapIndex : AbstractIndexCreationTask<Item>
		{
			public MaxNumberOfOutputs_MapIndex()
			{
				Map = items => from item in items 
							   from baz in item.Bazes
							   select new {item.Foo, baz};

				MaxIndexOutputsPerDocument = 20;
			}
		}

		[Fact]
		public void CanSpecifyMaxNumberOfOutputsForMapIndexByUsingIndexCreationTask()
		{
			using (var store = NewDocumentStore())
			{
				var index = new MaxNumberOfOutputs_MapIndex();
				store.ExecuteIndex(index);

				var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName);

				Assert.Equal(20, indexDefinition.MaxIndexOutputsPerDocument);
			}
		}

		public class MaxNumberOfOutputs_MapReduceIndex : AbstractIndexCreationTask<Item, MaxNumberOfOutputs_MapReduceIndex.ReduceResult>
		{
			public class ReduceResult
			{
				public string Foo { get; set; }
				public int Count { get; set; }
			}

			public MaxNumberOfOutputs_MapReduceIndex()
			{
				Map = items => from item in items select new { item.Foo, Count = item.Bar };
				Reduce = results => from result in results group result by result.Foo into g select new { Foo = g.Key, Count = g.Sum(x => x.Count) };
				MaxIndexOutputsPerDocument = 20;
			}
		}

		[Fact]
		public void CanSpecifyMaxNumberOfOutputsForMapReduceIndexByUsingIndexCreationTask()
		{
			using (var store = NewDocumentStore())
			{
				var index = new MaxNumberOfOutputs_MapReduceIndex();
				store.ExecuteIndex(index);

				var indexDefinition = store.DatabaseCommands.GetIndex(index.IndexName);

				Assert.Equal(20, indexDefinition.MaxIndexOutputsPerDocument);
			}
		}

		[Fact]
		public void CanSpecifyMaxNumberOfOutputsForMapIndexByUsingPutIndex()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("SampleIndex", new IndexDefinition()
				{
					Map =
						"from order in docs.Orders from line in order.Lines select new { Product = line.Product, Count = 1, Total = ((line.Quantity * line.PricePerUnit) *  ( 1 - line.Discount)) }",
					Reduce =
						"from result in results group result by result.Product into g select new { Product = g.Key, Count = g.Sum(x=>x.Count), Total = g.Sum(x=>x.Total)}",
					MaxIndexOutputsPerDocument = 20
				});

                var indexDefinition = store.DatabaseCommands.GetIndex("SampleIndex");

				Assert.Equal(20, indexDefinition.MaxIndexOutputsPerDocument);
			}
		}

		[Fact]
		public void MaxIndexOutputsPerDocumentSpecifiedForIndexTakesPriorityDuringIndexing()
		{
			using (var store = NewDocumentStore())
			{
				var index = new MaxNumberOfOutputs_MapIndex();
				store.ExecuteIndex(index);

				var item = new Item()
				{
					Foo = "foo",
					Bazes = new List<string>()
				};

				var globalLimit = store.DocumentDatabase.Configuration.MaxIndexOutputsPerDocument;

				Assert.True(index.MaxIndexOutputsPerDocument > globalLimit); // index limit should be greater than a global one

				var moreThanGlobalLimit = globalLimit + 1;

				Assert.True(moreThanGlobalLimit <= index.MaxIndexOutputsPerDocument); // we are going to produce fewer number of entries than the index limit

				for (int i = 0; i < moreThanGlobalLimit; i++)
				{
					item.Bazes.Add("baz/" + i.ToString());
				}

				using (var session = store.OpenSession())
				{
					session.Store(item);
					session.SaveChanges();
				}

				WaitForIndexing(store);

				var stats = store.DatabaseCommands.GetStatistics().Indexes.First(x => x.PublicName == index.IndexName);

				Assert.Equal(IndexingPriority.Normal, stats.Priority); // should not mark index as errored
			}
		}
	}
}