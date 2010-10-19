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
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            var data = PersistentDictionary.Read(JToken.FromObject("123"));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void AfterAddInDifferentTxValueDoesNotExists()
        {
            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            PersistentDictionary.ReadResult data = null;
            SupressTx(() => data = PersistentDictionary.Read(JToken.FromObject("123")));
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTx()
        {
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            var data = PersistentDictionary.Read(JToken.FromObject("123"));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }


        [Fact]
        public void AfterRollbackValueIsGoneToAllTx()
        {

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Rollback();

            Assert.Null(PersistentDictionary.Read(JToken.FromObject("123")));
            Assert.Null(PersistentDictionary.Read(JToken.FromObject("123")));
        }

        [Fact]
        public void AddReadAndThenAddWillNotCorruptData()
        {
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Assert.True(PersistentDictionary.Put(JToken.FromObject("789"), new byte[] { 3, 1, 4, 5 }));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, PersistentDictionary.Read(JToken.FromObject("123")).Data());

            Assert.True(PersistentDictionary.Put(JToken.FromObject("456"), new byte[] { 4, 5 }));

            Assert.Equal(new byte[] { 3, 1, 4, 5 }, PersistentDictionary.Read(JToken.FromObject("789")).Data());
        }
    }
}