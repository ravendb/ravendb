// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1233.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Exceptions;
	using Raven.Abstractions.Indexing;
	using Raven.Client.Document;
	using Raven.Client.Indexes;

	using Xunit;

	public class RavenDB_1233 : RavenTest
	{
		private class Person
		{
			public string Id { get; set; }

			public string AddressId { get; set; }

			public string Name { get; set; }
		}

		private class Address
		{
			public string Id { get; set; }

			public string Street { get; set; }
		}

		private class IndexWithLoadDocumentInReduce : AbstractIndexCreationTask<Person, IndexWithLoadDocumentInReduce.Result>
		{
			internal class Result
			{
				public string Street { get; set; }

				public string AddressId { get; set; }
			}

			public IndexWithLoadDocumentInReduce()
			{
				Map = people => from person in people
								select new
									   {
										   Street = string.Empty,
										   AddressId = person.AddressId
									   };

				Reduce = results => from result in results
									group result by result.AddressId into g
									select new
										   {
											   AddressId = g.Key,
											   Street = LoadDocument<Address>(g.Key).Street
										   };
			}
		}

		[Fact]
		public void ReduceCannotContainLoadDocumentMethods()
		{
			var e = Assert.Throws<IndexCompilationException>(
				() =>
				{
					using (var store = NewRemoteDocumentStore(fiddler: true))
					{
						var index = new IndexWithLoadDocumentInReduce();
						index.Execute(store);
					}
				});

			Assert.Equal("Reduce cannot contain LoadDocument() methods.", e.Message);

			e = Assert.Throws<IndexCompilationException>(
				() =>
				{
					using (var store = NewDocumentStore())
					{
						store.DatabaseCommands.PutIndex("IndexWithLoadDocumentInReduce",
							new IndexDefinition
							{
								Map = @"from person in docs.People select new { Street = string.Empty, AddressId = person.AddressId }",
								Reduce = @"from result in results group result by result.AddressId into g select new { AddressId = g.Key, Street = this.LoadDocument(g.Key).Street }"
							});
					}
				});

			Assert.Equal("Reduce cannot contain LoadDocument() methods.", e.Message);
		}
	}
}