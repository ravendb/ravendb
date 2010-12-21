//-----------------------------------------------------------------------
// <copyright file="Removes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class Removes : SimpleFileTest
    {
        [Fact]
        public void RemovingNonExistantIsNoOp()
        {
            Assert.True(Table.Remove(JToken.FromObject("a")));
        }

        [Fact]
        public void PutThenRemoveInSameTxWillResultInMissingValue()
        {
            

            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Assert.True(Table.Remove(JToken.FromObject("123")));

            var data = Table.Read(JToken.FromObject("123"));
            
            Assert.Null(data);
        }

        [Fact]
        public void BeforeCommitRemoveIsNotVisibleOutsideTheTx()
        {
            
            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            Assert.True(Table.Remove(JToken.FromObject("123")));

            Table.ReadResult data = null;
            SupressTx(() => data = Table.Read(JToken.FromObject("123")));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void AfterCommitRemoveIsVisibleOutsideTheTx()
        {
            Assert.True(Table.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            Assert.True(Table.Remove(JToken.FromObject("123")));

            Commit();

            Assert.Null(Table.Read(JToken.FromObject("123")));
        }
    }
}