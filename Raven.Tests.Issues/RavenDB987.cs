// -----------------------------------------------------------------------
//  <copyright file="RavenDB987.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB987 : RavenTest
	{
		public sealed class Categories_InUse_ByCity : AbstractIndexCreationTask<Restaurant, Categories_InUse_ByCity.Result>
		{
			public Categories_InUse_ByCity()
			{
				Map = restaurants => from r in restaurants
				                     from c in r.Categories.Union(new[] {r.MainCategory})
				                     let categ = LoadDocument<Category>(c)
				                     select new
				                     {
					                     categ.Id,
					                     categ.Name,
					                     categ.Icon,
					                     categ.Color,
					                     categ.Index,
					                     r.CityId,
					                     r.CityName,
				                     };

				Reduce = results => from r in results
				                    group r by r.Id
				                    into g
				                    let r = g.First()
				                    select new
				                    {
					                    r.Id,
					                    r.Name,
					                    r.Icon,
					                    r.Color,
					                    r.Index,
					                    r.CityId,
					                    r.CityName,
				                    };

				Sort(r => r.Index, SortOptions.Int);
			}

			public sealed class Result
			{
				public string Id { get; set; }

				public string Name { get; set; }

				public string Icon { get; set; }

				public string Color { get; set; }

				public int Index { get; set; }


				public string CityId { get; set; }

				public string CityName { get; set; }
			}
		}

		public class Category
		{
			public string Name { get; set; }
			public string Icon { get; set; }
			public string Color { get; set; }
			public int Index { get; set; }
			public string Id { get; set; }
		}

		public class Restaurant
		{
			public string Id { get; set; }
			public string MainCategory { get; set; }
			public List<string> Categories { get; set; }
			public string CityId { get; set; }
			public string CityName { get; set; }
		}

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.MaxNumberOfParallelIndexTasks = 1;
		}

		[Fact]
		public void ShouldGetAppropriateResults()
		{
			using (var store = NewDocumentStore())
			{
				new Categories_InUse_ByCity().Execute(store);

				using (var sesssion = store.OpenSession())
				{
					for (int i = 0; i < 6; i++)
					{
						sesssion.Store(new Category
						{
							Color = "red",
							Icon = "foo.jpg",
							Index = i,
							Name = "test " + i
						});
					}

					for (int i = 0; i < 3; i++)
					{
						sesssion.Store(new Restaurant
						{
							Categories = new List<string> { "categories/1", "categories/2", "categories/3", "categories/4", "categories/5" },
							CityId = "cities/2",
							CityName = "New York",
							MainCategory = "categories/6"
						});
					}

					sesssion.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var results = session.Query<Categories_InUse_ByCity.Result, Categories_InUse_ByCity>()
					       .Customize(x => x.WaitForNonStaleResults())
					       .ToList();

					Assert.Equal(6, results.Count);
					Assert.Empty(store.DocumentDatabase.Statistics.Errors);
				}
			}
		}
	}
}