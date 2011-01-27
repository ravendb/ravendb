//-----------------------------------------------------------------------
// <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Text;
using Newtonsoft.Json.Linq;
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

            database.Tables[0].UpdateKey(new JObject { { "id", 1 }, { "name", "ayende" } });
            database.Commit();

            database.BeginTransaction();

            database.Tables[0].UpdateKey(new JObject { { "id", 1 }, { "name", "oren" } });

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
				database.Tables[0].Put(new JObject { { "id", i } }, value);
				database.Commit();
			}


			for (int i = 0; i < count; i++)
			{
				var readResult = database.Tables[0].Read(new JObject {{"id", i}});
				Assert.Equal(value, readResult.Data());
			}

			database.Compact();


			for (int i = 0; i < count; i++)
			{
				var readResult = database.Tables[0].Read(new JObject { { "id", i } });
				Assert.Equal(value, readResult.Data());
			}
		}
    }
}