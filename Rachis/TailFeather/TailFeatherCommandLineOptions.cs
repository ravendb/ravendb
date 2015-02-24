using CommandLine;

namespace TailFeather
{
	public class TailFeatherCommandLineOptions
	{
		[Option('p', "port", Required = true, HelpText = "The http port to use")]
		public int Port { get; set; }

		[Option("bootstrap", HelpText = "Setup this node as the seed for first time cluster bootstrap")]
		public bool Boostrap { get; set; }

		[Option('d',"DataPath", HelpText = "Path for the node to use for persistent data", Required = true)]
		public string DataPath { get; set; }

		[Option('n', "Name", HelpText = "The friendly name of the node")]
		public string NodeName { get; set; }
	}
}