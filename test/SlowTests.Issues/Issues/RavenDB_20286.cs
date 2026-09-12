using System.IO;
using System.Linq;
using System.Reflection;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_20286 : NoDisposalNeeded
{
    public RavenDB_20286(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Core)]
    public void MustNotHaveMySqlConnectorUsing()
    {
        string currentDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        string server = Path.Combine(currentDir, "../../../../../src/Raven.Server");

        var csharpFiles = Directory.GetFiles(server, "*.cs", SearchOption.AllDirectories);

        Assert.True(csharpFiles.Length > 0);

        // MySQL-specific source files legitimately need a direct MySqlConnector dependency
        // (CDC sink binlog streaming, schema discovery, the migrator's MySQL provider). The
        // rule the rest of Raven.Server should follow is unchanged — only these contained
        // subtrees may import MySqlConnector.
        var mySqlSpecificPathFragments = new[]
        {
            Path.DirectorySeparatorChar + "MySQL" + Path.DirectorySeparatorChar,
            Path.DirectorySeparatorChar + "MySql",
        };

        foreach (string filePath in csharpFiles)
        {
            if (mySqlSpecificPathFragments.Any(fragment => filePath.Contains(fragment)))
                continue;

            using (var file = File.OpenText(filePath))
            {
                string line = file.ReadToEnd();

                Assert.DoesNotContain("using MySql.Data.MySqlClient", line);
                Assert.DoesNotContain("using MySqlConnector", line);
            }
        }
    }
}
