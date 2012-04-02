using System.IO;
using System;
using System.Management;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Raven.Database.Impl.Clustering
{
	public class ClusterInspecter
	{
		private delegate uint EnumeratedResourceCallback(IntPtr hSelf, IntPtr hEnum, IntPtr pParameter);

		private bool isRavenGenericServiceWorking;
		private bool checkedService;

		[DllImport("clusapi.dll", CharSet = CharSet.Unicode)]
		private static extern IntPtr OpenCluster(String lpszClusterName);

		[DllImport("clusapi.dll", CharSet = CharSet.Unicode)]
		private static extern bool CloseCluster(IntPtr hCluster);

		[DllImport("clusapi.dll", CharSet = CharSet.Unicode)]
		private static extern uint GetNodeClusterState(String lpszNodeName, ref uint lpcchClusterName);

		[DllImport("clusapi.dll", CharSet = CharSet.Unicode)]
		private static extern ClusterResourceState GetClusterResourceState(IntPtr hResource, String lpszNodeName, ref uint lpcchNodeName, String lpszGroupName, ref uint lpcchGroupName);

		private static NodeClusterState GetLocalNodeClusterState()
		{
			uint state = 0u;
			GetNodeClusterState(null, ref state);

			var clusterState = (NodeClusterState)Enum.ToObject(typeof(NodeClusterState), state);

			return clusterState;
		}

		private static IntPtr OpenLocalCluster()
		{
			return OpenCluster(null);
		}

		private static ClusterResourceState GetClusterResourceState(IntPtr hResource)
		{
			var a = 0u;

			return GetClusterResourceState(hResource, null, ref a, null, ref a);
		}

		[DllImport("ResUtils.dll", CharSet = CharSet.Auto, SetLastError = true)]
		private static extern uint ResUtilEnumResources(IntPtr hSelf, string lpszResTypeName, EnumeratedResourceCallback pResCallBack, IntPtr pParameter);

		[DllImport("kernel32.dll", SetLastError = true)]
		static extern IntPtr LocalFree(IntPtr hMem);

		[DllImport("clusapi.dll")]
		private static extern UIntPtr GetClusterResourceKey(IntPtr hResource, RegSam samDesired);

		[DllImport("clusapi.dll")]
		private static extern long ClusterRegCloseKey(UIntPtr hKey);

		[DllImport("clusapi.dll")]
		private static extern long ClusterRegOpenKey(UIntPtr hKey, [MarshalAs(UnmanagedType.LPWStr)] string lpszSubKey, RegSam samDesired, [Out] out UIntPtr phkResult);

		/// <summary>
		/// Returns a string value from the cluster database.
		/// </summary>
		/// <param name="hkeyClusterKey">Key identifying the location of the value in the cluster database.</param>
		/// <param name="pszValueName">A null-terminated Unicode string containing the name of the value to retrieve.</param>
		/// <returns>If the operation succeeds, the function returns a pointer to a buffer containing the string value.</returns>
		[DllImport("ResUtils.dll", CharSet = CharSet.Unicode)]
		private static extern IntPtr ResUtilGetSzValue(UIntPtr hkeyClusterKey, string pszValueName);

		public bool IsRavenRunningAsClusterGenericService()
		{
			if (checkedService)
				return isRavenGenericServiceWorking;

			var state = GetLocalNodeClusterState();

			if (state != NodeClusterState.ClusterStateRunning)
				return false;

			var clusterHandle = OpenLocalCluster();
			try
			{
				if (clusterHandle == IntPtr.Zero)
					return false;

				isRavenGenericServiceWorking = false;
				checkedService = false;

				var result = ResUtilEnumResources(IntPtr.Zero, "Generic Service", EnumResource, IntPtr.Zero);
				if (result != (uint)ErrorCodes.ERROR_SUCCESS)
					return false;
				checkedService = true;

				return isRavenGenericServiceWorking;
			}
			finally
			{
				if (clusterHandle != IntPtr.Zero)
				{
					CloseCluster(clusterHandle);
				}
			}
		}

		private uint EnumResource(IntPtr hSelf, IntPtr hEnum, IntPtr pParameter)
		{
			var genericServiceKey = GetClusterResourceKey(hEnum, RegSam.Read);
			try
			{
				UIntPtr parametersKey;
				ClusterRegOpenKey(genericServiceKey, "Parameters", RegSam.Read, out parametersKey);
				try
				{
					var pointerToServiceName = ResUtilGetSzValue(parametersKey, "ServiceName");
					try
					{
						string serviceName = Marshal.PtrToStringAuto(pointerToServiceName);

						var executableOfService = GetExecutableOfService(serviceName);

						if (executableOfService == null)
							return 0;

						executableOfService = Path.GetFileNameWithoutExtension(executableOfService.Trim('"'));

						var currentExecutable = Path.GetFileNameWithoutExtension(Assembly.GetEntryAssembly().Location);

						if (currentExecutable == executableOfService)
						{
							var clusterResourceState = GetClusterResourceState(hEnum);

							isRavenGenericServiceWorking = (clusterResourceState == ClusterResourceState.ClusterResourceInitializing ||
															clusterResourceState == ClusterResourceState.ClusterResourceOnlinePending ||
															clusterResourceState == ClusterResourceState.ClusterResourceOnline);

							return (uint)ErrorCodes.ERROR_NO_MORE_ITEMS; // halt enumeration
						}
					}
					finally
					{
						LocalFree(pointerToServiceName);
					}
				}
				finally
				{
					ClusterRegCloseKey(parametersKey);
				}
			}
			finally
			{
				ClusterRegCloseKey(genericServiceKey);
			}

			return 0;
		}

		public static string GetExecutableOfService(string serviceName)
		{
			var wqlObjectQuery = new WqlObjectQuery(string.Format("SELECT * FROM Win32_Service WHERE Name = '{0}'", serviceName));
			using (var managementObjectSearcher = new ManagementObjectSearcher(wqlObjectQuery))
			{
				var managementObjectCollection = managementObjectSearcher.Get();

				foreach (ManagementObject managementObject in managementObjectCollection)
				{
					return managementObject.GetPropertyValue("PathName").ToString();
				}
				return null;
			}
		}
	}
}