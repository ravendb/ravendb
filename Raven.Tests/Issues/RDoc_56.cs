// -----------------------------------------------------------------------
//  <copyright file="RDoc_56.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Collections.Generic;
	using System.Linq;

	using Raven.Abstractions.Data;
	using Raven.Client;
	using Raven.Client.Indexes;

	using Xunit;

	public class RDoc_56 : RavenTest
	{
		public class Post
		{
			public Post()
			{
				this.Comments = new List<Comment>();
				this.CommentsSet = new HashSet<Comment>();
			}

			public string Id { get; set; }

			public string Name { get; set; }

			public IList<Comment> Comments { get; set; }

			public ISet<Comment> CommentsSet { get; set; }
		}

		public class Comment
		{
			public Comment()
			{
				this.Comments = new List<Comment>();
				this.CommentsSet = new HashSet<Comment>();
			}

			public string Id { get; set; }

			public string Author { get; set; }

			public string Text { get; set; }

			public IList<Comment> Comments { get; set; }

			public ISet<Comment> CommentsSet { get; set; }
		}

		private class RecurseIndexWithIList : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithIList()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => x.Comments)
							   select new
									  {
										  comment.Text
									  };
			}
		}

		private class RecurseIndexWithICollection : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithICollection()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => (ICollection<Comment>)x.Comments)
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithIEnumerable : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithIEnumerable()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => x.Comments.AsEnumerable())
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithList : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithList()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => x.Comments.ToList())
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithArray : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithArray()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => x.Comments.ToArray())
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithISet : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithISet()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => x.CommentsSet)
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithHashSet : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithHashSet()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => (HashSet<Comment>)x.CommentsSet)
							   select new
							   {
								   comment.Text
							   };
			}
		}

		private class RecurseIndexWithSortedSet : AbstractIndexCreationTask<Post>
		{
			public RecurseIndexWithSortedSet()
			{
				Map = posts => from post in posts
							   from comment in Recurse(post, x => (SortedSet<Comment>)x.CommentsSet)
							   select new
							   {
								   comment.Text
							   };
			}
		}

		[Fact]
		public void IndexesShouldGetCreated()
		{
			using (var store = NewRemoteDocumentStore())
			{
				var array = new RecurseIndexWithArray();
				var hashSet = new RecurseIndexWithHashSet();
				var collection = new RecurseIndexWithICollection();
				var enumerable = new RecurseIndexWithIEnumerable();
				var iList = new RecurseIndexWithIList();
				var iSet = new RecurseIndexWithISet();
				var list = new RecurseIndexWithList();
				var sortedSet = new RecurseIndexWithSortedSet();

				array.Execute(store);
				hashSet.Execute(store);
				collection.Execute(store);
				enumerable.Execute(store);
				iList.Execute(store);
				iSet.Execute(store);
				list.Execute(store);
				sortedSet.Execute(store);

				var post = new Post { Id = "posts/1", Name = "Post 1" };
				var comments1 = new List<Comment>();
				var commentsSet1 = new HashSet<Comment>();

				var comments2 = new List<Comment>();
				var commentsSet2 = new HashSet<Comment>();

				comments2.Add(new Comment { Text = "Text 1" });
				comments2.Add(new Comment { Text = "Text 2" });
				commentsSet2.Add(new Comment { Text = "Text 1" });
				commentsSet2.Add(new Comment { Text = "Text 2" });

				comments1.Add(new Comment { Text = "Text 3" });
				comments1.Add(new Comment { Text = "Text 4", Comments = comments2, CommentsSet = commentsSet2 });
				commentsSet1.Add(new Comment { Text = "Text 3" });
				commentsSet1.Add(new Comment { Text = "Text 4", Comments = comments2, CommentsSet = commentsSet2 });

				post.Comments = comments1;
				post.CommentsSet = commentsSet1;

				using (var session = store.OpenSession())
				{
					session.Store(post);
					session.SaveChanges();
				}

				AssertIndexEntries(store, array.IndexName, 4);
				AssertIndexEntries(store, hashSet.IndexName, 4);
				AssertIndexEntries(store, collection.IndexName, 4);
				AssertIndexEntries(store, enumerable.IndexName, 4);
				AssertIndexEntries(store, iList.IndexName, 4);
				AssertIndexEntries(store, iSet.IndexName, 4);
				AssertIndexEntries(store, list.IndexName, 4);
				AssertIndexEntries(store, sortedSet.IndexName, 4);
			}
		}

		private void AssertIndexEntries(IDocumentStore store, string indexName, int expectedNumberOfResults)
		{
			using (var session = store.OpenSession())
			{
				session.Query<Post>(indexName)
						.Customize(x => x.WaitForNonStaleResults())
						.ToList();

				var arrayResult = store.DatabaseCommands.Query(indexName, new IndexQuery(), null, metadataOnly: false, indexEntriesOnly: true);

				Assert.Equal(expectedNumberOfResults, arrayResult.Results.Count);
			}
		}
	}
}