using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Basic
{
    public class DatabaseRecordDebugPackage : RavenTestBase
    {
        public DatabaseRecordDebugPackage(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EnsureIncludedInDatabaseRecordDebugPackage()
        {
            typeof(DatabaseRecord).GetProperties()
        }
    }
}
