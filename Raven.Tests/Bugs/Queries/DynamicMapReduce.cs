using System;
using Raven.Abstractions.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class DynamicMapReduce : RavenTest
	{
		[Fact]
		public void CanDynamicallyQueryOverItemCount()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = i % 2 == 0 ? "Ayende" : "Rahien" });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count)
						.WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(1, objects.Length);
					Assert.Equal("10", objects[0].Count);
				}
			}
		}

		[Fact]
		public void CanGroupByNestedProperty_Dynamic()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = new { First = i % 2 == 0 ? "Ayende" : "Oren" } });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count | AggregationOperation.Dynamic, "Name.First")
						.WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(2, objects.Length);

					Assert.Equal(5, objects[0].Count);
					Assert.Equal(5, objects[1].Count);
					Assert.Equal("Ayende", objects[0].NameFirst);
					Assert.Equal("Oren", objects[1].NameFirst);
				}
			}
		}

		[Fact]
		public void CanGroupByCollectionProperty_Dymamic()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Tags = new[] { new { Id = i % 2 } } });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count | AggregationOperation.Dynamic, "Tags,Id")
						.WaitForNonStaleResults()
						.ToArray();

					Assert.Equal(2, objects.Length);

					Assert.Equal(5, objects[0].Count);
					Assert.Equal(5, objects[1].Count);
					Assert.Equal("0", objects[0].TagsId);
					Assert.Equal("1", objects[1].TagsId);
				}
			}
		}

		[Fact]
		public void CanGroupByNestedProperty()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new {Name = new {First = i%2 == 0 ? "Ayende" : "Oren"}});
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count, "Name.First")
						.OrderBy("NameFirst")
						.ToArray();

					Assert.Equal(2, objects.Length);

					Assert.Equal("5", objects[0].Count);
					Assert.Equal("5", objects[1].Count);
					Assert.Equal("Ayende", objects[0].NameFirst);
					Assert.Equal("Oren", objects[1].NameFirst);
				}
			}
		}

		[Fact]
		public void CanGroupByCollectionProperty()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new {Tags = new[] {new {Id = i%2}}});
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count, "Tags,Id")
						.WaitForNonStaleResults(TimeSpan.FromMinutes(5))
						.ToArray();

					Assert.Equal(2, objects.Length);

					Assert.Equal("5", objects[0].Count);
					Assert.Equal("5", objects[1].Count);
					Assert.Equal("0", objects[0].TagsId);
					Assert.Equal("1", objects[1].TagsId);
				}
			}
		}

		[Fact]
		public void CanDynamicallyQueryOverItemCountByName()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = i % 2 == 0 ? "Ayende" : "Rahien" });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.GroupBy(AggregationOperation.Count, "Name")
						.WaitForNonStaleResults()
						.OrderBy("Name")
						.ToArray();


					Assert.Equal(2, objects.Length);
					Assert.Equal("5", objects[0].Count);
					Assert.Equal("Ayende", objects[0].Name);
					Assert.Equal("5", objects[1].Count);
					Assert.Equal("Rahien", objects[1].Name);
				}
			}
		}

		[Fact]
		public void CanDynamicallyQueryOverItemCountByNameWhileQueryingOnActive()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 10; i++)
					{
						s.Store(new { Name = i % 2 == 0 ? "Ayende" : "Rahien", Active = i % 3 == 0 });
					}
					s.SaveChanges();
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<dynamic>()
						.WhereEquals("Active",true)
						.GroupBy(AggregationOperation.Count | AggregationOperation.Dynamic, "Name")
						.WaitForNonStaleResults(TimeSpan.FromMinutes(3))
						.ToArray();


					Assert.Equal(2, objects.Length);
					Assert.Equal(2, objects[0].Count);
					Assert.Equal("Ayende", objects[0].Name);
					Assert.Equal(2, objects[1].Count);
					Assert.Equal("Rahien", objects[1].Name);
				}
			}
		}
	}
}
