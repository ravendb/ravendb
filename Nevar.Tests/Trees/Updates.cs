using Xunit;

namespace Nevar.Tests.Trees
{
	public class Updates : StorageTest
	{
		[Fact]
		public void CanAddAndUpdate()
		{
			using (var tx = Env.NewTransaction())
			{
				Env.Root.Add(tx, "test", StreamFor("1"));
				Env.Root.Add(tx, "test", StreamFor("2"));

				var readKey = ReadKey(tx, "test");
				Assert.Equal("test", readKey.Item1);
				Assert.Equal("2", readKey.Item2);
			}
		}
	}
}