using System;
using Newtonsoft.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class CanWorkWithTwoDicsInSameFile : MultiDicInSingleFile
    {
        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValues()
        {
            persistentDictionaryOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            persistentDictionaryTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommit()
        {
            

            persistentDictionaryOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            persistentDictionaryTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Commit();

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommitAndReopen()
        {
            

            persistentDictionaryOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            persistentDictionaryTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Database.Commit();

            Reopen();

            Assert.Equal(new byte[] { 1, 2, }, persistentDictionaryOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, persistentDictionaryTwo.Read(JToken.FromObject(1)).Data());
        }
    }
}