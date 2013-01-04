//-----------------------------------------------------------------------
// <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Text;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests.Bugs
{
	public class Compaction
	{
		[Fact]
		public void CanCompactWhenDataHasNoValue()
		{
			var database = new Database(new MemoryPersistentSource())
			{
				new Table(x => x.Value<string>("id"), "test")
			};

			database.BeginTransaction();

			database.Tables[0].UpdateKey(new RavenJObject { { "id", 1 }, { "name", "ayende" } });
			database.Commit();

			database.BeginTransaction();

			database.Tables[0].UpdateKey(new RavenJObject { { "id", 1 }, { "name", "oren" } });

			database.Commit();
			

			database.Compact();

		}

		[Fact]
		public void CanCompactThenReadingWithValue()
		{
			var database = new Database(new MemoryPersistentSource())
			{
				new Table(x => x.Value<string>("id"), "test")
			};

			var value = Encoding.UTF8.GetBytes(new string('$', 1024));

			var count = 330;
			for (int i = 0; i < count; i++)
			{
				database.BeginTransaction();
				database.Tables[0].Put(new RavenJObject { { "id", i } }, value);
				database.Commit();
			}


			for (int i = 0; i < count; i++)
			{
				database.BeginTransaction();
				var readResult = database.Tables[0].Read(new RavenJObject { { "id", i } });
				Assert.Equal(value, readResult.Data());
				database.Commit();
			}

			database.Compact();


			for (int i = 0; i < count; i++)
			{
				database.BeginTransaction();
				var readResult = database.Tables[0].Read(new RavenJObject { { "id", i } });
				Assert.Equal(value, readResult.Data());
				database.Commit();
			}
		}
	}
}
