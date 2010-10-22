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
            Assert.True(PersistentDictionary.Remove(JToken.FromObject("a")));
        }

        [Fact]
        public void PutThenRemoveInSameTxWillResultInMissingValue()
        {
            

            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Assert.True(PersistentDictionary.Remove(JToken.FromObject("123")));

            var data = PersistentDictionary.Read(JToken.FromObject("123"));
            
            Assert.Null(data);
        }

        [Fact]
        public void BeforeCommitRemoveIsNotVisibleOutsideTheTx()
        {
            
            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            Assert.True(PersistentDictionary.Remove(JToken.FromObject("123")));

            Table.ReadResult data = null;
            SupressTx(() => data = PersistentDictionary.Read(JToken.FromObject("123")));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void AfterCommitRemoveIsVisibleOutsideTheTx()
        {
            Assert.True(PersistentDictionary.Put(JToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

            Assert.True(PersistentDictionary.Remove(JToken.FromObject("123")));

            Commit();

            Assert.Null(PersistentDictionary.Read(JToken.FromObject("123")));
        }
    }
}