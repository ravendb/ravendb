//-----------------------------------------------------------------------
// <copyright file="AutoDetectAnalyzersForQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class AutoDetectAnalyzersForQuery : RavenTest
	{
		[Fact]
		public void WillDetectAnalyzerAutomatically()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test",
				                                new IndexDefinition
				                                {
				                                	Map = "from doc in docs select new { doc.Name}",
				                                	Indexes = {{"Name", FieldIndexing.NotAnalyzed}}
				                                });

				using(var session = store.OpenSession())
				{
					session.Store(new Foo{Name = "Ayende"});

					session.SaveChanges();
				}

				using(var session = store.OpenSession())
				{
					var foos = session.Advanced.DocumentQuery<Foo>("test")
						.Where("Name:Ayende")
						.WaitForNonStaleResults()
						.ToList();

					Assert.NotEmpty(foos);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}
		
	}
}
