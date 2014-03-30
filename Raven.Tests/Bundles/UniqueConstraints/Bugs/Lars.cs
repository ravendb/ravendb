using Raven.Client.UniqueConstraints;

using Xunit;

using System.Linq;

namespace Raven.Tests.Bundles.UniqueConstraints.Bugs
{
    public class TestRavenUniqueConstraints : UniqueConstraintsTest
	{
		public class WithUniqueName
		{
			[UniqueConstraint]
			public string Name { get; set; }
		}

		[Fact]
		public void It_should_not_be_possible_to_store_two_docs_with_the_same_unique_property()
		{

			using (var s = DocumentStore.OpenSession())
			{
				s.Store(new WithUniqueName { Name = "a" });
				s.SaveChanges();
			}

			var failed = false;
			try
			{
				using (var s = DocumentStore.OpenSession())
				{
					s.Store(new WithUniqueName { Name = "a" });
					s.SaveChanges();
				}
			}
			catch
			{
				// i don't know what exception to expect yet :-) 
				failed = true;
			}

			/* Notes by Felipe:
			 * DocsCount is returning 1 as expected. Why is this test a Bug?
			 */
			var docsCount = 0;
			using (var s = DocumentStore.OpenSession())
			{
				docsCount = s.Query<WithUniqueName>().Customize(x => x.WaitForNonStaleResults()).ToList().Count;
			}

			Assert.Equal(docsCount, 1); // Should have 1 doc only
			Assert.True(failed, "The second document should not be stored since it violates the unique constraint!");
		}
	}
}