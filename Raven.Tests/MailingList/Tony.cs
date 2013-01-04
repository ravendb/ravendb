// -----------------------------------------------------------------------
//  <copyright file="Tony.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Tony : RavenTest
	{
		[Fact]
		public void TestSortBys()
		{
			var values = new int[] { 3, 20, 100 };
			using(var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					foreach (var value in values)
					{
						var blog = new TestBlog { Title = "Test" + value, Weighting = value, Posts = new TestPost[value] };
						session.Store(blog);
					}
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var blogsInWeightingOrder = session.Query<TestBlog>()
						.Customize(x=>x.WaitForNonStaleResults())
						.OrderBy(b => b.Weighting).ToList();
					var blogsInPostsCountOrder = session.Query<TestBlog>()
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(b => b.Posts.Count).ToList();
					for (int i = 0; i < values.Length; i++)
					{
						Assert.Equal(values[i], blogsInWeightingOrder[i].Weighting); // pass: in numeric order 3, 20, 100
						Assert.Equal(values[i], blogsInPostsCountOrder[i].Posts.Count); // fail: in lexicographic order 100, 20, 3
					}
				}
			}
		}


		public class TestBlog
		{
			public int Id { get; set; }
			public string Title { get; set; }
			public int Weighting { get; set; }
			public IList<TestPost> Posts { get; set; }
		}

		public class TestPost
		{
			public string Subject { get; set; }
			public string Content { get; set; }
			public int Score { get; set; }
		}
	}
}