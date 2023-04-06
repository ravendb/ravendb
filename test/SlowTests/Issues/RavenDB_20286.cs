using System.IO;
using System.Reflection;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20286 : NoDisposalNeeded
{
    public RavenDB_20286(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void MustNotHaveMySqlConnectorUsing()
    {
        string currentDir = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
        string server = Path.Combine(currentDir, "../../../../../src/Raven.Server");

        var csharpFiles = Directory.GetFiles(server, "*.cs", SearchOption.AllDirectories);

        Assert.True(csharpFiles.Length > 0);

        foreach (string filePath in csharpFiles)
        {
            using (var file = File.OpenText(filePath))
            {
                string line = file.ReadToEnd();

                Assert.DoesNotContain("using MySql.Data.MySqlClient", line);
                Assert.DoesNotContain("using MySqlConnector", line);
            }
        }
    }
}
