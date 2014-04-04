//-----------------------------------------------------------------------
// <copyright file="CasingIssue.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Document
{
	public class CasingIssue : RavenTest
	{
		[Fact]
		public void CanQueryByEntityType()
		{
			using(var store = NewDocumentStore())
			using(var session = store.OpenSession())
			{
				session.Store(new Post{Title = "test", Body = "casing"});
				session.SaveChanges();

                var single = session.Advanced.DocumentQuery<Post>()
					.WaitForNonStaleResults()
					.Single();

				Assert.Equal("test", single.Title);
			}
		}

		[Fact]
		public void UnitOfWorkEvenWhenQuerying()
		{
			using (var store = NewDocumentStore())
			using (var session = store.OpenSession())
			{
				var entity = new Post { Title = "test", Body = "casing" };
				session.Store(entity);
				session.SaveChanges();

                var single = session.Advanced.DocumentQuery<Post>()
					.WaitForNonStaleResults()
					.Single();

				Assert.Same(entity, single);
			}
		}

		public class Post
		{
			public string Id { get; set; }
			public string Title { get; set; }
			public string Body { get; set; }
		}

	}
}