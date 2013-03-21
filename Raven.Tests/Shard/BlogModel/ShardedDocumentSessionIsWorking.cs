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
		public void MultiLoadShouldWork()
		{
			string id;
			using (var session = ShardedDocumentStore.OpenSession())
			{
				var entity = new User { Name = "Fitzchak Yitzchaki" };
				session.Store(entity);
				id = entity.Id;
				session.SaveChanges();
			}

			using (var session = ShardedDocumentStore.OpenSession())
			{
				var users = session.Load<User>(id, "does not exists");
				Assert.NotNull(users);
				Assert.Equal(2, users.Length);
				Assert.Equal("Fitzchak Yitzchaki", users[0].Name);

				Assert.Null(users[1]);
			}
		}
	}
}