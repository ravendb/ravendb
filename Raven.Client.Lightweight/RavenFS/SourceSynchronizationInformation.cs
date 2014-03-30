using System;

namespace Raven.Client.RavenFS
{
	public class SourceSynchronizationInformation
	{
		public Guid LastSourceFileEtag { get; set; }
		public string SourceServerUrl { get; set; }
		public Guid DestinationServerId { get; set; }

		public override string ToString()
		{
			return string.Format("LastSourceFileEtag: {0}", LastSourceFileEtag);
		}
	}
}