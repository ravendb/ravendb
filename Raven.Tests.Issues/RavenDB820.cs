// -----------------------------------------------------------------------
//  <copyright file="RavenDB820.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB820 : RavenTest
	{
		class Foo
		{
			public string First { get; set; }
		}

		class TestIndex : AbstractIndexCreationTask<Foo, TestIndex.QueryResult>
		{
			public class QueryResult
			{
				public string Query { get; set; }
			}

			public class ActualResult
			{
				public string[]Query { get; set; }
			}

			public TestIndex()
			{
				Map = docs => docs.Select(doc => new
				{
					Query = new object[]
                    {
                        doc.First
                    },
				});
				Index(org => org.Query, FieldIndexing.Analyzed);
				Store(org => org.Query, FieldStorage.Yes);
			}
		}

		[Fact]
		public void CanGetProjectionOfMixedContent()
		{
			using (var store = NewDocumentStore())
			{
				store.ExecuteIndex(new TestIndex());
				using (var session = store.OpenSession())
				{
					session.Store(new Foo { First = "foo" });
					session.Store(new Foo { First = "foo2" });
					session.SaveChanges();

					var a = session.Query<TestIndex.QueryResult, TestIndex>()
					               .Customize(c => c.WaitForNonStaleResults())
					               .Where(r => r.Query.StartsWith("foo"))
								   .AsProjection<TestIndex.ActualResult>()
					               .ToList();
					Assert.NotEmpty(a);
				}
			}
		}
	}
}