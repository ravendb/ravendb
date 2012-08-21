using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Data;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests
{
	public class ConcurrentPatching : LocalClientTest
	{
		[Fact]
		public void CanConcurrentlyUpdateSameDocument()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					s.Store(new Post {Comments = new List<Comment>()});
					s.SaveChanges();
				}

				int numberOfComments = 128;
				var patches = Enumerable.Range(0, numberOfComments).Select(x =>
				                                                           new PatchRequest
				                                                           {
					                                                           Name = "Comments",
					                                                           Type = PatchCommandType.Add,
					                                                           Value = RavenJToken.FromObject(new Comment
					                                                           {
						                                                           AuthorId = "ayende"
					                                                           })
				                                                           });

				Parallel.ForEach(patches, data => store.DatabaseCommands.Patch("posts/1", new[] {data}));

				using (var s = store.OpenSession())
				{
					Assert.Equal(numberOfComments, s.Load<Post>("posts/1").Comments.Count);
				}
			}
		}

		public class Post
		{
			public string Id { get; set; }
			public IList<Comment> Comments { get; set; }
		}

		public class Comment
		{
			public string AuthorId { get; set; }
		}
	}
}