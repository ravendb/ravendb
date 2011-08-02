using System.Linq;
using Microsoft.Web.Administration;

namespace Raven.Tests.Util
{
    public class IISConfig
    {
        public static void RemoveByApplicationPool(string applicationPoolName)
        {
            using(var manager = new ServerManager())
            {
                foreach (var site in manager.Sites)
                {
                    if (site.Applications.All(s => s.ApplicationPoolName == applicationPoolName) && site.Applications.Any())
                    {
                        site.Delete();
                    }
                }

                manager.CommitChanges();
            }

            using(var manager = new ServerManager())
            {
                var applicationPool = manager.ApplicationPools[applicationPoolName];
                if (applicationPool != null)
                {
                    applicationPool.Delete();
                    manager.CommitChanges();
                }
            }
        }

        public static void CreateApplicationPool(string applicationPoolName)
        {
            using(var manager = new ServerManager())
            {
                var pool = manager.ApplicationPools.Add(applicationPoolName);
                pool.ManagedPipelineMode = ManagedPipelineMode.Integrated;
                pool.ManagedRuntimeVersion = "v4.0";
                manager.CommitChanges();
            }
        }

        public static void CreateSite(string hostName, int port, string applicationPoolName, string phsyicalDirectory)
        {
            var siteName = applicationPoolName + "-" + hostName;

            using(var manager = new ServerManager())
            {
                var pool = manager.ApplicationPools[applicationPoolName];
                var site = manager.Sites.Add(siteName, "http", "*:" + port + ":" + hostName, phsyicalDirectory);
                site.ApplicationDefaults.ApplicationPoolName = applicationPoolName;
                manager.CommitChanges();
            }
        }
    }
}
