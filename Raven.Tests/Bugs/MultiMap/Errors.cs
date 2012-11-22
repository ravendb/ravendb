using System;
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.Bugs.MultiMap
{
	public class Errors : RavenTest
	{
		[Fact]
		public void MultiMapsMustHaveSameOutput()
		{
			using(var store = NewDocumentStore())
			{
				var exception = Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test",
				                                                                                                               new IndexDefinition
				                                                                                                               {
				                                                                                                               	Maps =
				                                                                                                               		{
				                                                                                                               			"from user in docs.Users select new { user.Username }",
				                                                                                                               			"from post in docs.Posts select new { post.Title }"
				                                                                                                               		}
				                                                                                                               }));

				Assert.Equal(@"Map functions defined as part of a multi map index must return identical types.
Baseline map		: from user in docs.Users select new { user.Username }
Non matching map	: from post in docs.Posts select new { post.Title }

Common fields		: __document_id
Missing fields		: Username
Additional fields	: Title", exception.Message);
			}
			
		}

		[Fact]
		public void MultiMapsMustHaveSameOutputAsReduce()
		{
			using (var store = NewDocumentStore())
			{
				var exception = Assert.Throws<InvalidOperationException>(() => store.DatabaseCommands.PutIndex("test",
				                                                                                               new IndexDefinition
				                                                                                               {
				                                                                                               	Maps =
				                                                                                               		{
				                                                                                               			"from user in docs.Users select new { user.Title }",
				                                                                                               			"from post in docs.Posts select new { post.Title }"
				                                                                                               		},
																													Reduce = "from result in results group result by result.Title into g select new { Title = g.Key, Count = 1 }"
				                                                                                               }));

				Assert.Equal(
					@"The result type is not consistent across map and reduce:
Common fields: Title
Map only fields   : 
Reduce only fields: Count
",
					exception.Message);
			}
		}
	}
}