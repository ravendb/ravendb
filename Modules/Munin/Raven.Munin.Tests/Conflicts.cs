//-----------------------------------------------------------------------
// <copyright file="Conflicts.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class Conflicts : SimpleFileTest
    {
        [Fact]
        public void TwoTxCannotAddSameDataBeforeCmmmit()
        {
            Assert.True(Table.Put(JToken.FromObject("a"), new byte[] { 1 }));

            SupressTx(() => Assert.False(Table.Put(JToken.FromObject("a"), new byte[] { 1 })));
        }

        [Fact]
        public void OneTxCannotDeleteTxThatAnotherTxAddedBeforeCommit()
        {
            Assert.True(Table.Put(JToken.FromObject("a"), new byte[] { 1 }));

            SupressTx(() => Assert.False(Table.Remove(JToken.FromObject("a"))));
        }


        [Fact]
        public void TwoTxCanAddSameDataAfterCmmmit()
        {
            Assert.True(Table.Put(JToken.FromObject("a"), new byte[] { 1 }));

            Commit();

            Assert.True(Table.Put(JToken.FromObject("a"), new byte[] { 1 }));
        }

        [Fact]
        public void OneTxCanDeleteTxThatAnotherTxAddedAfterCommit()
        {
            
            Assert.True(Table.Put(JToken.FromObject("a"), new byte[] { 1 }));

            Commit();

            Assert.True(Table.Remove(JToken.FromObject("a")));
        }
    }
}