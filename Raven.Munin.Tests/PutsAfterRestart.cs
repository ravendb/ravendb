using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
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
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));
            
            Reopen();

            var data = PersistentDictionary.Read(JToken.FromObject("123"));
            
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTxEvenAfterReopen()
        {
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));
            Assert.True(PersistentDictionary.Put(JToken.FromObject("431"), new byte[] { 1, 3, 4, 5 }));

            Commit();

            Reopen();

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, PersistentDictionary.Read(JToken.FromObject("123")).Data());
            Assert.Equal(new byte[] { 1, 3, 4, 5 }, PersistentDictionary.Read(JToken.FromObject("431")).Data());
        }
    }
}