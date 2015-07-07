// -----------------------------------------------------------------------
//  <copyright file="SetIndexPriority.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class SetIndexPriority : RavenTest
	{
		public class FakeIndex : AbstractIndexCreationTask
		{
			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinition()
				{
					Map = "from d in docs select new { d.Id }"
				};
			}
		}

		[Fact]
		public void changing_index_priority_needs_to_set_it_on_index_instance_as_well()
		{
			using (var store = NewDocumentStore())
			{
				new FakeIndex().Execute(store);

				foreach (var expected in new[] { IndexingPriority.Abandoned, IndexingPriority.Disabled, IndexingPriority.Error, IndexingPriority.Idle, IndexingPriority.Normal, })
				{
					store.DatabaseCommands.SetIndexPriority("FakeIndex", expected);

					var indexInstance = store.DocumentDatabase.IndexStorage.GetIndexInstance("FakeIndex");

					Assert.Equal(expected, indexInstance.Priority);
				}
			}
		}
	}
}