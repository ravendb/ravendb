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
            Assert.True(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 }));

            SupressTx(() => Assert.False(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 })));
        }

        [Fact]
        public void OneTxCannotDeleteTxThatAnotherTxAddedBeforeCommit()
        {
            Assert.True(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 }));

            SupressTx(() => Assert.False(PersistentDictionary.Remove(JToken.FromObject("a"))));
        }


        [Fact]
        public void TwoTxCanAddSameDataAfterCmmmit()
        {
            Assert.True(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 }));

            Commit();

            Assert.True(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 }));
        }

        [Fact]
        public void OneTxCanDeleteTxThatAnotherTxAddedAfterCommit()
        {
            
            Assert.True(PersistentDictionary.Put(JToken.FromObject("a"), new byte[] { 1 }));

            Commit();

            Assert.True(PersistentDictionary.Remove(JToken.FromObject("a")));
        }
    }
}