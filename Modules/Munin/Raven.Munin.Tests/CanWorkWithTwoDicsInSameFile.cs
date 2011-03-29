//-----------------------------------------------------------------------
// <copyright file="CanWorkWithTwoDicsInSameFile.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class CanWorkWithTwoDicsInSameFile : MultiDicInSingleFile
    {
        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValues()
        {
            tableOne.Put(RavenJToken.FromObject(1), new byte[] { 1, 2 });
			tableTwo.Put(RavenJToken.FromObject(1), new byte[] { 2, 3 });

			Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(RavenJToken.FromObject(1)).Data());
			Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(RavenJToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommit()
        {


			tableOne.Put(RavenJToken.FromObject(1), new byte[] { 1, 2 });
			tableTwo.Put(RavenJToken.FromObject(1), new byte[] { 2, 3 });

            Commit();

			Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(RavenJToken.FromObject(1)).Data());
			Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(RavenJToken.FromObject(1)).Data());
        }

        [Fact]
        public void StoringSameKeyInBothDicWithTwoDifferentValuesAfterCommitAndReopen()
        {
			tableOne.Put(RavenJToken.FromObject(1), new byte[] { 1, 2 });
			tableTwo.Put(RavenJToken.FromObject(1), new byte[] { 2, 3 });

            Database.Commit();

            Reopen();

			Assert.Equal(new byte[] { 1, 2, }, tableOne.Read(RavenJToken.FromObject(1)).Data());
			Assert.Equal(new byte[] { 2, 3 }, tableTwo.Read(RavenJToken.FromObject(1)).Data());
        }
    }
}