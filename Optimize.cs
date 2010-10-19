using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class Optimize : SimpleFileTest
    {
        [Fact]
        public void AfterManyModificationsFileSizeWillGoDownOnCommit()
        {
            
            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Remove(JToken.FromObject(i));
            }


            Commit();

            var oldSize = PersistentSource.Read(log => log.Length);
            PerformIdleTasks();

            Assert.True(oldSize > PersistentSource.Read(log => log.Length));
        }

        [Fact]
        public void AfterOptimizeCommittedDataIsStillThere()
        {
            
            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Remove(JToken.FromObject(i));
            }

            PersistentDictionary.Put(JToken.FromObject("a"), new byte[512]);


            Commit();
            var oldSize = PersistentSource.Read(log => log.Length);
            PerformIdleTasks();

            Assert.True(oldSize > PersistentSource.Read(log => log.Length));

            Assert.NotNull(
                PersistentDictionary.Read(JToken.FromObject("a"))
                );
        }


        [Fact]
        public void AfterOptimizeUnCommittedDataIsStillThere()
        {
            
            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                PersistentDictionary.Remove(JToken.FromObject(i));
            }

            var txId2 = Guid.NewGuid();
            PersistentDictionary.Put(JToken.FromObject("a"), new byte[512]);


            Commit();
            var oldSize = PersistentSource.Read(log => log.Length);
            PerformIdleTasks();

            Assert.True(oldSize > PersistentSource.Read(log => log.Length));

            Assert.NotNull(
                PersistentDictionary.Read(JToken.FromObject("a"))
                );
        }
    }
}