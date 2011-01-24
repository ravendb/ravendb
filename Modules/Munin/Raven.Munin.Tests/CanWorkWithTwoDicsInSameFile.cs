//-----------------------------------------------------------------------
// <copyright file="CanWorkWithTwoDicsInSameFile.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
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
            tableOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            tableTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(JToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommit()
        {
            

            tableOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            tableTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Commit();

            Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(JToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommitAndReopen()
        {
            

            tableOne.Put(JToken.FromObject(1), new byte[] { 1, 2 });
            tableTwo.Put(JToken.FromObject(1), new byte[] { 2, 3 });

            Database.Commit();

            Reopen();

            Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(JToken.FromObject(1)).Data());
            Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(JToken.FromObject(1)).Data());
        }
    }
}