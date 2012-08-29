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
	public class MapReduceWithDifferentFieldNames : RavenTest
	{
		[Fact]
		public void WhenTheAnonymousTypeResultIsNotTheSame_ShouldThrowAnException()
		{
			using (var store = NewDocumentStore())
			{
				Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Field1 = 1, Field2NameDoesNotMatch = 1 }",
					Reduce = "from result in results group result by \"constant\" into g select new { Field1NameDoesNotMatch = g.Sum(x => x.Field1), Field2 = g.Sum(x => x.Field2) }"
				}));

				Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Field1 = 1, Field2 = 1 }",
					Reduce = "from result in results group result by \"constant\" into g select new { Field1NameDoesNotMatch = g.Sum(x => x.Field1), Field2 = g.Sum(x => x.Field2) }"
				}));

				Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Field1 = 1, Field2NameDoesNotMatch = 1 }",
					Reduce = "from result in results group result by \"constant\" into g select new { Field1 = g.Sum(x => x.Field1), Field2 = g.Sum(x => x.Field2) }"
				}));
			}
		}

		[Fact]
		public void WhenTheAnonymousTypeResultIsTheSame_ShouldNotThrowAnException()
		{
			using (var store = NewDocumentStore())
			{
				Assert.DoesNotThrow(() => store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { Field1 = 1, Field2 = 1 }",
					Reduce = "from result in results group result by \"constant\" into g select new { Field1 = g.Sum(x => x.Field1), Field2 = g.Sum(x => x.Field2) }"
				}));
			}
		}
	}
}