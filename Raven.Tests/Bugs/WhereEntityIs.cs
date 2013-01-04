//-----------------------------------------------------------------------
// <copyright file="WhereEntityIs.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Bugs
{
	public class WhereEntityIs : RavenTest
	{
		[Fact]
		public void Can_query_using_multiple_entities()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinitionBuilder<object>()
				{
					Map = docs => from i in docs.WhereEntityIs<Animal>("Cats", "Dogs")
								  select new {i.Color}
				}.ToIndexDefinition(store.Conventions));

				using(var s = store.OpenSession())
				{
					s.Store(new Cat
					{
						Color = "Black",
						Mewing = true
					});

					s.Store(new Dog
					{
						Barking = false,
						Color = "Black"
					});

					s.SaveChanges();
				}

				using(var s = store.OpenSession())
				{
					var animals = s.Query<Animal>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Color == "Black")
						.ToList();

					Assert.Equal(2, animals.Count);
				}
			}
		}

		[Fact]
		public void Can_query_using_multiple_entities_using_natural_syntax()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinitionBuilder<Animal>()
				{
					Map = docs => from i in docs.WhereEntityIs<Animal>("Cats", "Dogs")
								  select new { i.Color }
				}.ToIndexDefinition(store.Conventions));

				using (var s = store.OpenSession())
				{
					s.Store(new Cat
					{
						Color = "Black",
						Mewing = true
					});

					s.Store(new Dog
					{
						Barking = false,
						Color = "Black"
					});

					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var animals = s.Query<Animal>("test")
						.Customize(x => x.WaitForNonStaleResults())
						.Where(x => x.Color == "Black")
						.ToList();

					Assert.Equal(2, animals.Count);
				}
			}
		}

		public class Animal
		{
			public string Id { get; set; }
			public string Color { get; set; }
		}

		public class Dog : Animal
		{
			public bool Barking { get; set; }
		}

		public class Cat : Animal
		{
			public bool Mewing { get; set; }
		}
	}
}
