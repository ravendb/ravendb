//-----------------------------------------------------------------------
// <copyright file="Removes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Json.Linq;
using Xunit;

namespace Raven.Munin.Tests
{
    public class Removes : SimpleFileTest
    {
        [Fact]
        public void RemovingNonExistantIsNoOp()
        {
			Assert.True(Table.Remove(RavenJToken.FromObject("a")));
        }

        [Fact]
        public void PutThenRemoveInSameTxWillResultInMissingValue()
        {


			Assert.True(Table.Put(RavenJToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

			Assert.True(Table.Remove(RavenJToken.FromObject("123")));

			var data = Table.Read(RavenJToken.FromObject("123"));
            
            Assert.Null(data);
        }

        [Fact]
        public void BeforeCommitRemoveIsNotVisibleOutsideTheTx()
        {

			Assert.True(Table.Put(RavenJToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

			Assert.True(Table.Remove(RavenJToken.FromObject("123")));

            Table.ReadResult data = null;
			SupressTx(() => data = Table.Read(RavenJToken.FromObject("123")));

            Assert.Equal(new byte[] { 1, 2, 4, 5 }, data.Data());
        }

        [Fact]
        public void AfterCommitRemoveIsVisibleOutsideTheTx()
        {
			Assert.True(Table.Put(RavenJToken.FromObject("123"), new byte[] { 1, 2, 4, 5 }));

            Commit();

			Assert.True(Table.Remove(RavenJToken.FromObject("123")));

            Commit();

			Assert.Null(Table.Read(RavenJToken.FromObject("123")));
        }
    }
}