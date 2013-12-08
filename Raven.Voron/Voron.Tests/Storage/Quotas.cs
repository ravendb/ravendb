// -----------------------------------------------------------------------
//  <copyright file="Quotas.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.IO;
using Voron.Exceptions;
using Xunit;

namespace Voron.Tests.Storage
{
	public class Quotas : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxStorageSize = 1024 * 1024 * 1; // 1MB
		}

		[Fact]
		public void ShouldThrowQuotaException()
		{
			var quotaEx = Assert.Throws<QuotaException>(() =>
			{
				// everything in one transaction
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					for (int i = 0; i < 1024; i++)
					{
						tx.State.Root.Add(tx, "items/" + i, new MemoryStream(new byte[1024]));
					}

					tx.Commit();
				}
			});
		}
	}
}