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
    }
}