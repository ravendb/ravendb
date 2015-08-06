using System;
using System.Linq;
using System.Security.AccessControl;

namespace Raven.Monitor.IO.Data
{
	internal class ResourceToMonitor : IComparable<ResourceToMonitor>
	{
		public PathInformation[] Paths { private get; set; }

		public string ResourceName { get; set; }

		public ResourceType ResourceType { get; set; }

		public int CompareTo(ResourceToMonitor other)
		{
			if (string.Equals(other.ResourceName, ResourceName, StringComparison.OrdinalIgnoreCase) && other.ResourceType == ResourceType)
				return 0;

			return -1;
		}

		public PathInformation GetMatchingPath(string path)
		{
			return Paths.FirstOrDefault(p => path.StartsWith(p.Path, StringComparison.OrdinalIgnoreCase));
		}

		public bool IsMatch(string path)
		{
			return Paths.Any(p => path.StartsWith(p.Path, StringComparison.OrdinalIgnoreCase));
		}
	}
}