using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Storage.Managed;
using Xunit;
using System.Linq;

namespace Raven.Storage.Tests
{
	public class General : TxStorageTest
	{
		[Fact]
		public void CanGetNewIdentityValue()
		{
			using (var tx = new TransactionalStorage("test"))
			{
				tx.Write(mutator =>
				{
					Assert.Equal(1, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(1, mutator.General.GetNextIdentityValue("rahien"));

					Assert.Equal(2, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(2, mutator.General.GetNextIdentityValue("rahien"));	
				});


				tx.Write(mutator =>
				{
					Assert.Equal(3, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(3, mutator.General.GetNextIdentityValue("rahien"));

					Assert.Equal(4, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(4, mutator.General.GetNextIdentityValue("rahien"));
				});
			}
		}

	}
}