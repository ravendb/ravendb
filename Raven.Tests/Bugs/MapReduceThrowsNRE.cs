//-----------------------------------------------------------------------
// <copyright file="MapReduceThrowsNRE.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class MapReduceThrowsNRE : RavenTest
	{
		[Fact]
		public void CanQueryWithoutGettingNullRefException()
		{
			using (var store = NewDocumentStore())
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new TypeCatalog(typeof(ChildrenMapReduceIndex))), store);

				Seed(store);

				using (var s = store.OpenSession())
				{
					var childrenReduceResults = s.Query<ChildreReduceResult, ChildrenMapReduceIndex>()
						.Customize(x => x.WaitForNonStaleResults()).
						ToList();

					Assert.NotEmpty(childrenReduceResults);
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}

		#region Temperaments enum

		public enum Temperaments
		{
			Sanguine = 0,
			Choleric = 1,
			Melancholic = 2,
			Phlegmatic = 3,
		}

		#endregion

		public void Seed(IDocumentStore store)
		{
			const string name = "John Doe";
			const string friend = "Stever Rogers";
			var rand = new Random();

			for (int i = 0; i < 10; i++)
			{
				using (IDocumentSession session = store.OpenSession())
				{
					var person = new Person { Name = name };

					if (i % 2 == 0)
					{
						person.Friends = new List<Friend> { new Friend { Name = friend } };
					}

					session.Store(person);

					// add children
					for (int j = 0; j < 5; j++)
					{
						var child = new Children
						{
							Name = name + " jr.",
							Parent = person.Id,
							// keep Phlegmatic from being set in a child
							Temperament = (Temperaments)rand.Next(0, 2)
						};

						session.Store(child);
					}

					session.SaveChanges();
				}
			}
		}

		#region Nested type: Children

		public class Children
		{
			public string Id { get; set; }
			public string Parent { get; set; }
			public string Name { get; set; }
			public Temperaments Temperament { get; set; }
		}

		#endregion

		#region Nested type: ChildrenMapReduceIndex

		public class ChildrenMapReduceIndex : AbstractIndexCreationTask
		{
			public override string IndexName
			{
				get { return "ChildrenMapReduceIndex"; }
			}

			public override IndexDefinition CreateIndexDefinition()
			{
				return new IndexDefinitionBuilder<Children, ChildreReduceResult>
				{
					Map = children => from child in children
									  select new
									  {
										  child.Parent,
										  Sanguine = child.Temperament == Temperaments.Sanguine ? 1 : 0,
										  Choleric = child.Temperament == Temperaments.Choleric ? 1 : 0,
										  Melancholic = child.Temperament == Temperaments.Melancholic ? 1 : 0,
										  Phlegmatic = child.Temperament == Temperaments.Phlegmatic ? 1 : 0
									  },
					Reduce = results => from result in results
										group result by result.Parent
											into g
											select new
											{
												Parent = g.Key,
												Sanguine = g.Sum(x => x.Sanguine),
												Choleric = g.Sum(x => x.Choleric),
												Melancholic = g.Sum(x => x.Melancholic),
												Phlegmatic = g.Sum(x => x.Phlegmatic)
											}
				}.ToIndexDefinition(Conventions);
			}
		}

		#endregion

		public class ChildreReduceResult
		{
			public string Parent { get; set; }
			public int Sanguine { get; set; }
			public int Choleric { get; set; }
			public int Melancholic { get; set; }
			public int Phlegmatic { get; set; }
		}

		#region Nested type: ChildrenReduceResult

		public class ChildrenMapResult
		{
			public string Parent { get; set; }
			public Temperaments Temperament { get; set; }
		}

		#endregion

		#region Nested type: Friend

		public class Friend
		{
			public string Name { get; set; }
		}

		#endregion

		#region Nested type: Person

		public class Person
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public List<Friend> Friends { get; set; }
		}

		#endregion
	}
}
