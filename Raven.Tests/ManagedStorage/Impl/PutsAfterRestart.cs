using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage.Impl
{
    public class PutsAfterRestart : SimpleFileTest
    {
        [Fact]
        public void CanStartAndStopPersistentDictionary()
        {
            Reopen();
        }

        [Fact]
        public void RestartBeforeTxCommitMeansNoData()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));
            
            Reopen();

            var data = persistentDictionary.Read(JToken.FromObject("123"), txId);
            
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTxEvenAfterReopen()
        {
            var txId = Guid.NewGuid();

            Assert.True(persistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }, txId));
            Assert.True(persistentDictionary.Put(JToken.FromObject("431"), new byte[] { 1, 3, 4, 5 }, txId));

            Commit(txId);

            Reopen();

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, persistentDictionary.Read(JToken.FromObject("123"), Guid.NewGuid()).Data());
            Assert.Equal(new byte[] { 1, 3, 4, 5 }, persistentDictionary.Read(JToken.FromObject("431"), Guid.NewGuid()).Data());
        }
    }
}