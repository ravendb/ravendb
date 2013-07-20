using System.Collections.Generic;
using Xunit;

namespace Nevar.Tests.Trees
{
	public class Deletes : StorageTest
	{
		 [Fact]
		 public void CanDeleteAtRoot()
		 {
			 using (var tx = Env.NewTransaction())
			 {
				 for (int i = 0; i < 1000; i++)
				 {
					 Env.Root.Add(tx, string.Format("{0,5}",i), StreamFor("abcdefg"));
				 }
				 tx.Commit();
			 }

			 using (var tx = Env.NewTransaction())
			 {
				 for (int i = 0; i < 15; i++)
				 {
					 Env.Root.Delete(tx, string.Format("{0,5}", i));
				 }
				 tx.Commit();
			 }

			 var expected = new List<Slice>();
			 for (int i = 15; i < 1000; i++)
			 {
				 expected.Add(string.Format("{0,5}", i));
			 }

			 using (var tx = Env.NewTransaction())
			 {
				 var list = Env.Root.KeysAsList(tx);
				 Assert.Equal(expected, list);
			 }
		 }
	}
}