//-----------------------------------------------------------------------
// <copyright file="General.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Xunit;

namespace Raven.Tests.Storage
{
	public class General : RavenTest
	{
		[Fact]
		public void CanGetNewIdentityValue()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					Assert.Equal(1, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(1, mutator.General.GetNextIdentityValue("rahien"));

					Assert.Equal(2, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(2, mutator.General.GetNextIdentityValue("rahien"));	
				});


				tx.Batch(mutator =>
				{
					Assert.Equal(3, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(3, mutator.General.GetNextIdentityValue("rahien"));

					Assert.Equal(4, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(4, mutator.General.GetNextIdentityValue("rahien"));
				});
			}
		}

		[Fact]
		public void TransactionStorageIdRemainsConstantAcrossRestarts()
		{
			Guid id;
			using (var tx = NewTransactionalStorage())
			{
				Assert.Equal(tx.Id, tx.Id);
				id = tx.Id;
			}

			using (var tx = NewTransactionalStorage())
			{
				Assert.Equal(id, tx.Id);
			}
		}

		[Fact]
		public void CanGetNewIdentityValueAfterRestart()
		{
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator =>
				{
					Assert.Equal(1, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(1, mutator.General.GetNextIdentityValue("rahien"));

					Assert.Equal(2, mutator.General.GetNextIdentityValue("ayende"));
					Assert.Equal(2, mutator.General.GetNextIdentityValue("rahien"));
				});
			}

			using(var tx = NewTransactionalStorage())
			{

				tx.Batch(mutator =>
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
