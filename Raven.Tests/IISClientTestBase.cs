using System.IO;
using Raven.Client;
using Raven.Database.Extensions;

namespace Raven.Tests
{
    public abstract class IISClientTestBase
    {
        protected const string WebDirectory = @".\RavenIISTestWeb\";

        public static string DeployWebProjectToTestDirectory()
        {
            var fullPath = Path.GetFullPath(WebDirectory);
            if (!Directory.Exists(fullPath))
            {
                IOExtensions.CopyDirectory(GetRavenWebSource(), WebDirectory);
            }
            return fullPath;
        }

        private static string GetRavenWebSource()
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

        public abstract IDocumentStore GetDocumentStore();
    }
}