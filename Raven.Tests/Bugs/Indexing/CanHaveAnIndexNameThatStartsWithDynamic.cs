using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
	public class CanHaveAnIndexNameThatStartsWithDynamic : RavenTest
	{
		public class SomeDoc
		{
			public string MyStringProp { get; set; }
		}

		public class SomeOtherDoc
		{
			public int MyIntProp { get; set; }
		}

		public class DynamicIndex : AbstractIndexCreationTask<SomeDoc, DynamicIndex.Result>
		{
			public class Result
			{
				public string MyStringProp { get; set; }
				public int Count { get; set; }
			}

			public DynamicIndex()
			{
				Map = docs => from doc in docs
							  select new { MyStringProp = doc.MyStringProp, Count = 1 };

				Reduce = results => from result in results
									group result by result.MyStringProp
										into g
										select new { MyStringProp = g.Key, Count = g.Sum(x => x.Count) };

			}
		}

		[Fact]
		public void CanHaveAnIndexWithANameThatStartsWithTheWordDynamic()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					var one = new SomeDoc { MyStringProp = "Test" };
					var two = new SomeDoc { MyStringProp = "two" };
					var other = new SomeOtherDoc { MyIntProp = 1 };
					s.Store(one);
					s.Store(two);
					s.Store(other);
					s.SaveChanges();
					new DynamicIndex().Execute(store);
					var list = s.Query<DynamicIndex.Result, DynamicIndex>().Customize(x => x.WaitForNonStaleResultsAsOfNow()).ToList();
					Assert.NotEqual(0, list.Count());
				}
			}
		}
	}
}
