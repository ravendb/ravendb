using Rachis.Storage;

namespace Rachis.Commands
{
	public class TopologyChangeCommand : Command
	{
		public Topology Requested { get; set; }
		public Topology Previous { get; set; }
	}
}