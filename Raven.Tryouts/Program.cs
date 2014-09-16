using System;
using Raven.Client.Embedded;
using Raven.Client.FileSystem;
using Raven.Database.Config;
using Raven.Server;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			var ravenConfiguration = new RavenConfiguration
			{
				DataDirectory = "~/Data"
			};
			var ravenDbServer = new RavenDbServer(ravenConfiguration);
			
		}
	}
}