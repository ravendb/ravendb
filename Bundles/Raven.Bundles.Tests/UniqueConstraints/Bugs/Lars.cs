using Raven.Client.UniqueConstraints;
using Xunit;

namespace Raven.Bundles.Tests.UniqueConstraints.Bugs
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

			Assert.True(failed, "The second document should not be stored since it violates the unique constraint!");
		}
	}
}