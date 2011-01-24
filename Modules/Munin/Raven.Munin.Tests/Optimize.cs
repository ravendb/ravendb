//-----------------------------------------------------------------------
// <copyright file="Optimize.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
                Table.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                Table.Remove(JToken.FromObject(i));
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
                Table.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                Table.Remove(JToken.FromObject(i));
            }

            Table.Put(JToken.FromObject("a"), new byte[512]);


            Commit();
            var oldSize = PersistentSource.Read(log => log.Length);
            PerformIdleTasks();

            Assert.True(oldSize > PersistentSource.Read(log => log.Length));

            Assert.NotNull(
                Table.Read(JToken.FromObject("a"))
                );
        }


        [Fact]
        public void AfterOptimizeUnCommittedDataIsStillThere()
        {
            
            for (int i = 0; i < 16; i++)
            {
                Table.Put(JToken.FromObject(i), new byte[512]);
            }

            for (int i = 0; i < 16; i++)
            {
                Table.Remove(JToken.FromObject(i));
            }

            var txId2 = Guid.NewGuid();
            Table.Put(JToken.FromObject("a"), new byte[512]);


            Commit();
            var oldSize = PersistentSource.Read(log => log.Length);
            PerformIdleTasks();

            Assert.True(oldSize > PersistentSource.Read(log => log.Length));

            Assert.NotNull(
                Table.Read(JToken.FromObject("a"))
                );
        }
    }
}