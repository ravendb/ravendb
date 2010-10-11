using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage.Impl
{
    public class Removes : SimpleFileTest
    {
        [Fact]
        public void RemovingNonExistantIsNoOp()
        {
            Assert.True(persistentDictionary.Remove(JToken.FromObject("a"), Guid.NewGuid()));
        }

        [Fact]
        public void PutThenRemoveInSameTxWillResultInMissingValue()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            Assert.True(persistentDictionary.Remove(JToken.FromObject("123"), txId));

            var data = persistentDictionary.Read(JToken.FromObject("123"), txId);
            
            Assert.Null(data);
        }

        [Fact]
        public void BeforeCommitRemoveIsNotVisibleOutsideTheTx()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            Commit(txId);

            Assert.True(persistentDictionary.Remove(JToken.FromObject("123"), Guid.NewGuid()));

            var data = persistentDictionary.Read(JToken.FromObject("123"), txId);

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void AfterCommitRemoveIsVisibleOutsideTheTx()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));

            Commit(txId);

            txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Remove(JToken.FromObject("123"), txId));

            Commit(txId);

            Assert.Null(persistentDictionary.Read(JToken.FromObject("123"), txId));
        }
    }
}