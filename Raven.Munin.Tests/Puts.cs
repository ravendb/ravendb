//-----------------------------------------------------------------------
// <copyright file="Puts.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class Puts : SimpleFileTest
    {
        [Fact]
        public void CanStartAndStopPersistentDictionary()
        {
            // all work happens in ctor & dispose
        }

        [Fact]
        public void CanAddAndGetDataSameTx()
        {
            
            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            var data = Table.Read(JToken.FromObject("123"));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void UpdatingTree()
        {

            for (int i = 0; i < 11; i++)
            {
                Table.Put("docs/" + i, new byte[] {1, 2, 4, 5});
            }

            Table.Put("docs/0", new byte[] { 5,4,3,2,1 });
            
            var data = Table.Read("docs/0");

            Assert.Equal(new byte[] { 5, 4, 3, 2, 1 }, data.Data());
        }

        [Fact]
        public void AfterAddInDifferentTxValueDoesNotExists()
        {
            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Table.ReadResult data = null;
            SupressTx(() => data = Table.Read(JToken.FromObject("123")));
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTx()
        {
            

            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            var data = Table.Read(JToken.FromObject("123"));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }


        [Fact]
        public void AfterRollbackValueIsGoneToAllTx()
        {

            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Rollback();

            Assert.Null(Table.Read(JToken.FromObject("123")));
            Assert.Null(Table.Read(JToken.FromObject("123")));
        }

        [Fact]
        public void AddReadAndThenAddWillNotCorruptData()
        {
            

            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Assert.True(Table.Put(JToken.FromObject("789"), new byte[] { 3, 1, 4, 5 }));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, Table.Read(JToken.FromObject("123")).Data());

            Assert.True(Table.Put(JToken.FromObject("456"), new byte[] { 4, 5 }));

            Assert.Equal(new byte[] { 3, 1, 4, 5 }, Table.Read(JToken.FromObject("789")).Data());
        }
    }
}