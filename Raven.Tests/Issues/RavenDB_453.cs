// -----------------------------------------------------------------------
//  <copyright file="RavenDB_453.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Tests.Bugs;
using Xunit;
using Raven.Client.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_453 : RavenTest
	{
		[Fact]
		public void WillGetErrorWhenTryingToUseMultipleConcurrentAsyncOps()
		{
			using(var store = NewRemoteDocumentStore())
			{
				using(var s = store.OpenAsyncSession())
				{
					var listAsync1 = s.Query<Item>()
						.Where(x=>x.Version == "1")
						.ToListAsync();
					Assert.Throws<InvalidOperationException>(
						() => s.Query<User>().ToListAsync());

					listAsync1.Wait();
				}
			}
		}
	}
}