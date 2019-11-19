//-----------------------------------------------------------------------
// <copyright file="Expiration.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Documents.Expiration;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Core.Expiration
{
    public class ExpirationStressTest : NoDisposalNoOutputNeeded
    {
        public ExpirationStressTest(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(1000)]
        [InlineData(10000)]
        public async Task CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(int count)
        {
            using (var expiration = new ExpirationTests(Output))
            {
                await expiration.CanAddALotOfEntitiesWithSameExpiry_ThenReadItBeforeItExpires_ButWillNotBeAbleToReadItAfterExpiry(count);
            }
        }
    }
}
