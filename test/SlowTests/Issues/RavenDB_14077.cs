using System.IO;
using FastTests;
using Raven.Server.Web.Studio;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_14077 : RavenTestBase
    {
        [Fact]
        public void GetDataDirectoryInfoCommand_ShouldReturnRootedPath()
        {
            using (var store = GetDocumentStore())
            {
                var requestExecutor = store.GetRequestExecutor();

                var path = "..\\..\\";
                var dataDirectoryInfo = new DataDirectoryInfo.GetDataDirectoryInfoCommand(path, store.Database, isBackup: true);

                using (var context = JsonOperationContext.ShortTermSingleUse())
                {
                    requestExecutor.Execute(dataDirectoryInfo, context);

                    var fullPath = dataDirectoryInfo.Result.FullPath;

                    Assert.True(Path.IsPathRooted(fullPath));
                }
            }
        }
    }
}
