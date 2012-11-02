namespace Raven.Tests.Issues
{
	using Raven.Bundles.Tests.Replication;

	using Xunit;

	public class RavenDB_576 : ReplicationBase
	{
		public class Person
		{
		}

		[Fact]
		public void ReplicationShouldWorkWhenCompressionIsEnabled()
		{
			var store1 = CreateStore(enableCompressionBundle: true);
			var store2 = CreateStore(enableCompressionBundle: true);

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenSession())
			{
				session.Store(new Person());
				session.SaveChanges();
			}

			this.WaitForDocument<Person>(store2, "people/1");
		}
	}
}