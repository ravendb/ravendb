using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Raven.Tests.Common.Util
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
					var applications = ((IEnumerable<dynamic>)site.Applications);
					if (applications.All(s => s.ApplicationPoolName == applicationPoolName) && applications.Any())
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

		public static void CreateSite(string hostName, int port, string applicationPoolName, string physicalDirectory)
		{
			var siteName = applicationPoolName + "-" + hostName;

			using (dynamic manager = Assembly.CreateInstance("Microsoft.Web.Administration.ServerManager"))
			{
				var pool = manager.ApplicationPools[applicationPoolName];
				var site = manager.Sites.Add(siteName, "http", "*:" + port + ":" + hostName, physicalDirectory);
				site.ApplicationDefaults.ApplicationPoolName = applicationPoolName;
				manager.CommitChanges();
			}
		}
	}
}
