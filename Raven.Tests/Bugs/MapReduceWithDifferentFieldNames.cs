// -----------------------------------------------------------------------
//  <copyright file="MapReduceWithDifferentFieldNames.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MapReduceWithDifferentFieldNames : LocalClientTest
	{
		[Fact]
		public void WhenTheAnonymousTypeResultIsNotTheSame_ShouldThrowAnException()
		{
			using (var store = NewDocumentStore())
			{
				Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Name, Count = 1 }",
					Reduce = "from result in results group result by result.Name into g select new { g.Key, Count = g.Sum(x=>x.Count) }"
				}));
			}
		}
	}
}