//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests.Server.Documents.Expiration;
using Xunit;

namespace SlowTests.Core.Bundles
{
    public class ExpirationStressTest
    {
        [Theory]
        [InlineData(10)]
        [InlineData(1000)]
        [InlineData(10000)]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(int count)
        {
            using (var expiration = new Expiration())
            {
                await expiration.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(count);
            }
        }
    }
}