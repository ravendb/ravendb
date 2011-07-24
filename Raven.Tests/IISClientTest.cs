using System.IO;
using Raven.Client;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Tests.Util;

namespace Raven.Tests
{
    public class IISClientTest : WithNLog
    {
        const string HostName = "RavenIISTest";
        const int Port = 80;
        const string ApplicationPool = "RavenIISTest";
        protected const string WebDirectory = @".\RavenIISTestWeb\";
        protected const string DbDirectory = @".\RavenIISTestDb\";

        public IISClientTest()
        {
            IISConfig.RemoveByApplicationPool(ApplicationPool);

            IOExtensions.DeleteDirectory(WebDirectory);
            IOExtensions.DeleteDirectory(DbDirectory);
        }

        public IDocumentStore GetDocumentStore()
        {
            if (!Directory.Exists(WebDirectory))
            {
                var fullPath = Path.GetFullPath(WebDirectory);

                IOExtensions.CopyDirectory(GetRavenWebSource(), WebDirectory);

                PSHostsFile.HostsFile.Set(HostName, "127.0.0.1");
                IISConfig.CreateApplicationPool(ApplicationPool);
                IISConfig.CreateSite(HostName, Port, ApplicationPool, fullPath);
            }

            return new DocumentStore()
            {
                Url = string.Format("http://{0}:{1}/", HostName, Port)
            };
        }

        string GetRavenWebSource()
        {
            foreach (var path in new[] { @".\..\..\..\Raven.Web", @".\_PublishedWebsites\Raven.Web" })
            {
                var fullPath = Path.GetFullPath(path);
                
                if (Directory.Exists(fullPath) && Directory.Exists(Path.Combine(fullPath, "bin")))
                {
                    return fullPath;
                }
            }

            throw new FileNotFoundException("Could not find source directory for Raven.Web");
        }
    }
}