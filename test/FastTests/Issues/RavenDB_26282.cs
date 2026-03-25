using System;
using System.IO;
using System.Linq;
using Raven.Server.Utils.Debugging;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues;

public class RavenDB_26282 : NoDisposalNeeded
{
    public RavenDB_26282(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void RavenDebugExec_DevBuildLookupPaths_ShouldResolveToExistingFile()
    {
        // Skip the first path (".") - that is the production layout where Raven.Debug
        // sits next to Raven.Server. In a dev build it won't exist there.
        // We validate that at least one of the dev lookup paths resolves to a real file.
        var devPaths = RavenDebugExecUtils.FileSystemLookupPaths.Skip(1).ToList();

        var found = devPaths
            .Select(p => Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, p, RavenDebugExecUtils.RavenDebugFileName)))
            .FirstOrDefault(File.Exists);

        Assert.True(found != null,
            $"Raven.Debug executable was not found in any dev lookup path relative to '{AppContext.BaseDirectory}'. " +
            $"Running on .NET {Environment.Version} (net{Environment.Version.Major}.0). " +
            $"If the TFM changed, update the hardcoded lookup paths in {nameof(RavenDebugExecUtils)}. " +
            $"Paths tried: {Environment.NewLine}{string.Join(Environment.NewLine, devPaths)}");
    }
}
