using System.Collections.Generic;
using Raven.Client.Embedded;

namespace LinQTests
{
	public class JoinedChildTransport
	{
		public string ChildId { get; set; }
		public string TransportId { get; set; }
		public string Name { get; set; }
	}
}
