// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3365.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Util;
using Raven.Client.Embedded;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3365 : RavenTest
	{
		[Fact]
		public void index_pretty_printer_ignores_whitespaces()
		{
			var firstFormat = IndexPrettyPrinter.TryFormat("from order in docs.Orders select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }");
			var secondFormat = IndexPrettyPrinter.TryFormat("from order  \t   in docs.Orders       select new { order.Company, Count = 1, Total = order.Lines.Sum(l=>(l.Quantity * l.PricePerUnit) *  ( 1 - l.Discount)) }");

			Assert.Equal(firstFormat, secondFormat);
		}

		[Fact]
		public void shouldnt_reset_index_when_non_meaningful_change()
		{
			using (var store = NewDocumentStore())
			{
				Setup(store);

				// now fetch index definition modify map (only by giving extra write space)
				var indexName = new RavenDB_3248_TestObject_ByName().IndexName;
				var indexDef = store.DatabaseCommands.GetIndex(indexName);

				indexDef.Map = "   " + indexDef.Map.Replace(" ", "  \t ") + "   ";
				store.DatabaseCommands.PutIndex(indexName, indexDef, true);

				// and verify if index wasn't reset
				var statsForIndex = store.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(i => i.Name == indexName);
				Assert.NotNull(statsForIndex);
				Assert.True(statsForIndex.IndexingSuccesses > 0);
			}
		}
	
		[Fact]
		public void shouldnt_reset_index_when_max_doc_output_changed()
		{
			using (var store = NewDocumentStore())
			{
				Setup(store);

				// now fetch index definition modify map (only by giving extra write space)
				var indexName = new RavenDB_3248_TestObject_ByName().IndexName;
				var indexDef = store.DatabaseCommands.GetIndex(indexName);

				indexDef.MaxIndexOutputsPerDocument = 45;
				store.DatabaseCommands.PutIndex(indexName, indexDef, true);

				// and verify if index wasn't reset
				var statsForIndex = store.DatabaseCommands.GetStatistics().Indexes.FirstOrDefault(i => i.Name == indexName);
				Assert.NotNull(statsForIndex);
				Assert.True(statsForIndex.IndexingSuccesses > 0);
			}
		}

		private static void Setup(EmbeddableDocumentStore store)
		{
			new RavenDB_3248_TestObject_ByName().Execute(store);
			using (var session = store.OpenSession())
			{
				for (int i = 0; i < 20; i++)
				{
					session.Store(new RavenDB_3248_TestObject());
				}
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DatabaseCommands.Admin.StopIndexing();
		}

	}
}