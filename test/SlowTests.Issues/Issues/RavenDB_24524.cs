using System.IO;
using FastTests;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24524 : NoDisposalNeeded
{
    public RavenDB_24524(ITestOutputHelper output) : base(output)
    {
    }

    [RavenMultiplatformFact(RavenTestCategory.Core, RavenPlatform.Windows)]
    public void MoveDirectory_ShouldSucceed_WhenSourceAndDestinationDifferOnLongPathPrefix()
    {
        var root = Path.Combine(Path.GetTempPath(), "RavenDB_24524_" + Path.GetRandomFileName());
        var srcDir = Path.Combine(root, "src");
        var dstDir = Path.Combine(root, "dst");

        try
        {
            Directory.CreateDirectory(srcDir);
            File.WriteAllText(Path.Combine(srcDir, "marker.txt"), "hello");

            // Force the mismatch: pass one path with the \\?\ prefix and the other without.
            // Without the fix, Directory.Move throws because Path.GetPathRoot returns different
            // roots for "\\?\D:\..." vs "D:\...".
            var srcWithPrefix = @"\\?\" + srcDir;
            var dstWithoutPrefix = dstDir;

            IOExtensions.MoveDirectory(srcWithPrefix, dstWithoutPrefix);

            Assert.False(Directory.Exists(srcDir), "Source directory should no longer exist after move.");
            Assert.True(Directory.Exists(dstDir), "Destination directory should exist after move.");
            Assert.Equal("hello", File.ReadAllText(Path.Combine(dstDir, "marker.txt")));

            // And the symmetric case: dst prefixed, src not.
            var srcDir2 = Path.Combine(root, "src2");
            var dstDir2 = Path.Combine(root, "dst2");
            Directory.CreateDirectory(srcDir2);
            File.WriteAllText(Path.Combine(srcDir2, "marker.txt"), "world");

            IOExtensions.MoveDirectory(srcDir2, @"\\?\" + dstDir2);

            Assert.False(Directory.Exists(srcDir2));
            Assert.True(Directory.Exists(dstDir2));
            Assert.Equal("world", File.ReadAllText(Path.Combine(dstDir2, "marker.txt")));
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }
}
