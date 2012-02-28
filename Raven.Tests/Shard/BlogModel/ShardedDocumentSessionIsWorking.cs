// -----------------------------------------------------------------------
//  <copyright file="ShardedDocumentSessionIsWorking.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Xunit;

namespace Raven.Tests.Shard.BlogModel
{
	public class ShardedDocumentSessionIsWorking : ShardingScenario
	{
		[Fact]
		public void LoadShouldReturnTheCorrectUserWhenExists()
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				var user = session.Load<User>("users/1");
				Assert.NotNull(user);
				Assert.Equal("Fitzchak Yitzchaki", user.Name);
			}
		}

		[Fact]
		public void LoadShouldReturnNullForNotExistsUser()
		{
			using (var session = shardedDocumentStore.OpenSession())
			{
				var user = session.Load<User>("users/2");
				Assert.Null(user);
			}
		} 
	}
}