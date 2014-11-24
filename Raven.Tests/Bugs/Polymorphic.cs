//-----------------------------------------------------------------------
// <copyright file="Polymorphic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Collections.Generic;
using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class Polymorphic : RavenTest
	{
		public interface IVegetable
		{
		}

		public class Carrot : IVegetable
		{
			public decimal Orangeness { get; set; }
		}

		public class Potato : IVegetable
		{
			public decimal Mushiness { get; set; }
		}

		public class Recipe
		{
			public string Id { get; set; }

			public IList<IVegetable> SideDishes { get; set; }
		}

		[Fact]
		public void CanSaveAndLoadPolymorphicList()
		{
			using (var store = NewDocumentStore())
			{
				store.Conventions.CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All;

				using (var session = store.OpenSession())
				{
					session.Store(new Recipe
					{
						SideDishes = new List<IVegetable>
			            {
			                new Potato{ Mushiness = 1.43m},
			                new Carrot{Orangeness = 23.3m},
			            }
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var recipe = session.Load<Recipe>("recipes/1");

					Assert.IsType<Potato>(recipe.SideDishes[0]);
					Assert.IsType<Carrot>(recipe.SideDishes[1]);
				}
			}
		}
	}
}
