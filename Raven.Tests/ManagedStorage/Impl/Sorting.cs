using System;
using System.Linq;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage.Impl
{
    public class Sorting : SimpleFileTest
    {
        [Fact]
        public void CanAskForFirstItemOnEmpty()
        {
            Assert.Null(persistentDictionary.First);
        }


        [Fact]
        public void CanAskForLastItemOnEmpty()
        {
            Assert.Null(persistentDictionary.Last);
        }

        [Fact]
        public void CanGetFirstKey()
        {
            var txId = Guid.NewGuid();
            persistentDictionary.Add(JToken.FromObject(1), new byte[] {1, 2}, txId);

            Commit(txId);

            Assert.Equal(1, persistentDictionary.First.Value<int>());
        }

        [Fact]
        public void CanGetLastKey()
        {
            var txId = Guid.NewGuid();
            persistentDictionary.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);
            persistentDictionary.Add(JToken.FromObject(2), new byte[] { 1, 2 }, txId);
            persistentDictionary.Add(JToken.FromObject(0), new byte[] { 1, 2 }, txId);
            Commit(txId);

            Assert.Equal(2, persistentDictionary.Last.Value<int>());
        }

        [Fact]
        public void CanScan()
        {
            var txId = Guid.NewGuid();
            persistentDictionary.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);
            persistentDictionary.Add(JToken.FromObject(2), new byte[] { 1, 2 }, txId);
            persistentDictionary.Add(JToken.FromObject(0), new byte[] { 1, 2 }, txId);
            Commit(txId);

            var actual = persistentDictionary.GreaterThanOrEqual(JToken.FromObject(1)).ToArray();
            Assert.Equal(2, actual.Length);
            Assert.Equal(JToken.FromObject(1), actual[0], new JTokenEqualityComparer());
            Assert.Equal(JToken.FromObject(2), actual[1], new JTokenEqualityComparer());
        }

        [Fact]
        public void LastvalueIsFirstValueWhenThereIsOnlyOne()
        {
            var txId = Guid.NewGuid();
            persistentDictionary.Add(JToken.FromObject(1), new byte[] { 1, 2 }, txId);

            Commit(txId);

            Assert.Equal(1, persistentDictionary.Last.Value<int>());
            Assert.Equal(1, persistentDictionary.First.Value<int>());
        }
    }
}