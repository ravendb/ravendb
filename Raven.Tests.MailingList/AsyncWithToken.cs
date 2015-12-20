// -----------------------------------------------------------------------
//  <copyright file="AsyncWithToken.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class AsyncWithToken : RavenTest
    {
        public class Item
        {
            public bool Active;
        }
        [Fact]
        public async Task CanUseSameTokenMulitpleTimes()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item()).ConfigureAwait(false);
                    await session.SaveChangesAsync().ConfigureAwait(false);
                }
                using (var session = store.OpenAsyncSession())
                {
                    var cts = new CancellationTokenSource();

                    await session.LoadAsync<Item>("items/1", cts.Token).ConfigureAwait(false);

                    await session.Query<Item>().SingleAsync(x=>x.Active == false,cts.Token).ConfigureAwait(false);

                }
            }
        }
    }
}