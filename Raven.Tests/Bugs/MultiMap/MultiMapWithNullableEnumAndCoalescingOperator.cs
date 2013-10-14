using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Raven.Client.Linq.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class MultiMapWithNullableEnumAndCoalescingOperator : RavenTest
	{
		[Fact]
		public void Can_create_index()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Obj1 { Name = "Tom", MyEnumField = MyEnum.OtherValue });
					session.Store(new Obj1 { Name = "Oscar" });

					session.SaveChanges();
				}

				new MySearchIndexTask().Execute(store);

				WaitForIndexing(store);

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);

				using (var s = store.OpenSession())
				{
					Assert.NotEmpty(s.Query<Obj1, MySearchIndexTask>()
										.Where(x => x.MyEnumField == MyEnum.OtherValue)
										.ToList());

					Assert.NotEmpty(s.Query<Obj1, MySearchIndexTask>()
										.Where(x => x.Name == "Oscar")
										.ToList());

				}
			}
		}

		public enum MyEnum
		{
			Default = 0,
			OtherValue = 1,
			YetAnotherValue = 2
		}

		public class Tag
		{
			public string Name { get; set; }
		}

		public class Obj1
		{
			public string Id { get; set; }
			public string Name { get; set; }
			public string Description { get; set; }
			public MyEnum? MyEnumField { get; set; }
			public Tag[] Tags { get; set; }
			public bool IsDeleted { get; set; }
		}

		public class MySearchIndexTask : AbstractMultiMapIndexCreationTask<MySearchIndexTask.Result>
		{
			public class Result
			{
				public object[] Content { get; set; }
				public string Name { get; set; }
				public MyEnum MyEnumField { get; set; }
			}

			public override string IndexName { get { return "MySearchIndexTask"; } }
			public MySearchIndexTask()
			{
				AddMap<Obj1>(items => from item in items
									  where item.IsDeleted == false
									  select new Result
									  {
										  Name = item.Name,
										  Content = new object[] { item.Name.Boost(3), item.Tags.Select(x => x.Name).Boost(2), item.Description },
										  MyEnumField = item.MyEnumField ?? MyEnum.Default
									  });

				Index(x => x.Content, FieldIndexing.Analyzed);
				Index(x => x.Name, FieldIndexing.Default);
				Index(x => x.MyEnumField, FieldIndexing.Default);
			}
		}

	}
}