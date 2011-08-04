using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Raven.Tests.Util
{
    public class IISConfig
    {
    	private static Assembly _assembly;

    	public static Assembly Assembly
    	{
    		get
    		{
    			if (_assembly != null)
					return _assembly;
				return _assembly = Assembly.LoadFile(@"C:\Windows\System32\InetSrv\Microsoft.Web.Administration.dll");
    		}
    	}

    	public static void RemoveByApplicationPool(string applicationPoolName)
        {
        	using(dynamic manager = Assembly.CreateInstance("Microsoft.Web.Administration.ServerManager"))
            {
                foreach (var site in manager.Sites)
                {
					if (((IEnumerable<dynamic>)site.Applications).All(s => s.ApplicationPoolName == applicationPoolName) && site.Applications.Any())
                    {
                        site.Delete();
                    }
                }

                manager.CommitChanges();
            }

            using(dynamic manager = Assembly.CreateInstance("Microsoft.Web.Administration.ServerManager"))
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
			using (dynamic manager = Assembly.CreateInstance("Microsoft.Web.Administration.ServerManager"))
			{
                var pool = manager.ApplicationPools.Add(applicationPoolName);
				pool.ManagedPipelineMode = 0; // ManagedPipelineMode.Integrated;
                pool.ManagedRuntimeVersion = "v4.0";
                manager.CommitChanges();
            }
        }

        public static void CreateSite(string hostName, int port, string applicationPoolName, string phsyicalDirectory)
        {
            var siteName = applicationPoolName + "-" + hostName;

			using (dynamic manager = Assembly.CreateInstance("Microsoft.Web.Administration.ServerManager"))
			{
                var pool = manager.ApplicationPools[applicationPoolName];
                var site = manager.Sites.Add(siteName, "http", "*:" + port + ":" + hostName, phsyicalDirectory);
                site.ApplicationDefaults.ApplicationPoolName = applicationPoolName;
                manager.CommitChanges();
            }
        }
    }
}
