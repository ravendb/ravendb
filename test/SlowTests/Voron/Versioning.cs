using System;
using System.IO;
using Xunit;

namespace SlowTests.Voron
{
    public class Versioning : StorageTest
	{
		[Fact]
		public void SplittersAndRebalancersShouldNotChangeNodeVersion()
		{
			const int DocumentCount = 100000;

			var rand = new Random();
			var testBuffer = new byte[123];
			rand.NextBytes(testBuffer);


			using (var tx = Env.WriteTransaction())
			{
				tx.CreateTree("tree1");
				tx.Commit();
			}

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree1");
                for (var i = 0; i < DocumentCount; i++)
                {
                    tree.Add("Foo" + i, new MemoryStream(testBuffer));

                }
                tx.Commit();
            }

			using (var snapshot = Env.WriteTransaction())
			{
				for (var i = 0; i < DocumentCount; i++)
				{
				    var readTree = snapshot.CreateTree("tree1");
				    var result = readTree.Read("Foo" + i);
					readTree.Delete("Foo" + i, result.Version);
				}
			}
		}
	}
}