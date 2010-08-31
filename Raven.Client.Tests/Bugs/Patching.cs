using System.Collections.Generic;
using Raven.Database.Data;
using Raven.Database.Json;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class Patching : BaseClientTest
	{
		public class Post
		{
			public string Id { get; set; }
			public List<Comment> Comments { get; set; }
		}

		public class Comment
		{
			public string AuthorId { get; set; }
		}

		[Fact]
		public void CanModifyValue()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					s.Store(new Post
					{
						Comments = new List<Comment>
						{
							new Comment{ AuthorId = "authors/123"}
						}
					});
					s.SaveChanges();
				}

				store.DatabaseCommands.Batch(new[]
				{
					new PatchCommandData
					{
						Key = "posts/1",
						Patches = new PatchRequest[]
						{
							new PatchRequest
							{
								Name = "Comments",
								Type = PatchCommandType.Modify,
								Position = 0,
								Nested = new PatchRequest[]
								{
									new PatchRequest
									{
										Name = "AuthorId",
										Type = PatchCommandType.Set,
										Value = "authors/456"

									},
								}
							},
						}
					}
				});

				using (var s = store.OpenSession())
				{
					Assert.Equal("authors/456", s.Load<Post>("posts/1").Comments[0].AuthorId);
				}
			}
		}
	}
}