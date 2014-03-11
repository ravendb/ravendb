using System;
using Raven.Client.RavenFS;

namespace Raven.Abstractions.RavenFS
{
	public class SynchronizationDetails
	{
		public string FileName { get; set; }
		public Guid FileETag { get; set; }
		public string DestinationUrl { get; set; }
		public SynchronizationType Type { get; set; }
	}
}