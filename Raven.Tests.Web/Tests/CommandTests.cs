// -----------------------------------------------------------------------
//  <copyright file="CommandTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;

using Raven.Tests.Common.Attributes;

namespace Raven.Tests.Web.Tests
{
    public class CommandTests : WebTestBase
    {
        [IISExpressInstalledFact]
        public async Task Sync()
        {
            await TestControllerAsync("SyncCommands");
        }

        [IISExpressInstalledFact]
        public async Task Async()
        {
            await TestControllerAsync("AsyncCommands");
        }

        [IISExpressInstalledFact]
        public async Task Mixed()
        {
            await TestControllerAsync("MixedCommands");
        }
    }
}
