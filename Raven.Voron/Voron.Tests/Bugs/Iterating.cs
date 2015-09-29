// -----------------------------------------------------------------------
//  <copyright file="Iterating.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Voron.Tests.Bugs
{
	public class Iterating : StorageTest
	{
		[Fact]
		public void IterationShouldNotFindAnyRecordsAndShouldNotThrowWhenNumberOfEntriesOnPageIs1AndKeyDoesNotMatch()
		{
			using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
			{
				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					env.CreateTree(tx, "tree");

					tx.Commit();
				}

				using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = tx.ReadTree("tree");
					tree.Add(@"Raven\Database\1", StreamFor("123"));

					tx.Commit();
				}

				using (var snapshot = env.CreateSnapshot())
				using (var iterator = snapshot.Iterate("tree"))
				{
					Assert.False(iterator.Seek(@"Raven\Filesystem\"));
				}
			}
		}
	}
}