//-----------------------------------------------------------------------
// <copyright file="PutsAfterRestart.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Json.Linq;
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


			Assert.True(Table.Put(RavenJToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));
            
            Reopen();

			var data = Table.Read(RavenJToken.FromObject("123"));
            
            Assert.Null(data);
        }

        [Fact]
        public void AfterCommitValueIsVisibleToAllTxEvenAfterReopen()
        {


			Assert.True(Table.Put(RavenJToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));
			Assert.True(Table.Put(RavenJToken.FromObject("431"), new byte[] { 1, 3, 4, 5 }));

            Commit();

            Reopen();

			Assert.Equal(new byte[] { 1, 2, 4, 5 }, Table.Read(RavenJToken.FromObject("123")).Data());
			Assert.Equal(new byte[] { 1, 3, 4, 5 }, Table.Read(RavenJToken.FromObject("431")).Data());
        }
    }
}