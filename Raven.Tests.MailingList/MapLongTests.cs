using System.Collections.Generic;
using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class MapLongTests : RavenTestBase
	{
		[Fact]
		public void CanMapADictionaryLong()
		{
			using (var store = NewDocumentStore())
			{
				new FooIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					session.Store(new Foo
					{
						Something = "cat",
						Items = new Dictionary<long, Bar>()
						{
							{534553454, new Bar()},
							{634553454, new Bar()}
						},
						Long = 5345435435435
					});

					session.SaveChanges();
				}

				WaitForIndexing(store);
				using (var session = store.OpenSession())
				{
					var foos =
						session
							.Query<FooIndex.Result, FooIndex>()
							.Where(x => x.DynamicKey == 634553454)
							.OfType<Foo>()
							.ToList();

					Assert.Equal(1, foos.Count);
				}
			}
		}

		public class Foo
		{
			public string Id { get; set; }
			public string Something { get; set; }
			public IDictionary<long, Bar> Items { get; set; }
			public long Long { get; set; }
		}

		public class Bar
		{
			public string Whatever { get; set; }
		}

		public class FooIndex : AbstractIndexCreationTask<Foo, FooIndex.Result>
		{
			public class Result
			{
				public string Something { get; set; }
				public long Key { get; set; }
				public string Whatever { get; set; }
				public long Long { get; set; }
				public long DynamicKey { get; set; }
			}

			public FooIndex()
			{
				Map = foos => from foo in foos
							  from item in foo.Items
							  select new
							  {
								  foo.Something,
								  Key = item.Key,
								  Whatever = item.Value.Whatever,
								  foo.Long,
								  _ = CreateField("DynamicKey", item.Key, false, false)
							  };
			}
		}
	}
}