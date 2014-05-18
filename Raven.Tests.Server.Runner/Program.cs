namespace Raven.Tests.Server.Runner
{
	using System;
	using System.Threading;

	using NDesk.Options;

	public class Program
	{
		private readonly OptionSet optionSet;

		private int port = 8585;

		private Program()
		{
			optionSet = new OptionSet
				            {
								{ "port:", "Change default port (8585).", s => port = int.Parse(s) },
					            { "h|?|help", v => PrintUsageAndExit(0) },
				            };
		}

		public static void Main(string[] args)
		{
			try
			{
				var program = new Program();
				program.Parse(args);
			}
			catch (Exception e)
			{
				Console.WriteLine("Unexpected error: " + e.StackTrace);
			}
		}

		private void Parse(string[] args)
		{
			// Do these arguments the traditional way to maintain compatibility
			//if (args.Length < 3)
			//	PrintUsageAndExit(-1);

			try
			{
				optionSet.Parse(args);
			}
			catch (Exception e)
			{
				PrintUsageAndExit(e);
			}

			var runner = new ServerRunner(port);
			runner.Start();

			while (runner.IsRunning)
			{
				Thread.Sleep(1000);
			}
		}

		private void PrintUsageAndExit(Exception e)
		{
			Console.WriteLine(e);
			PrintUsageAndExit(-1);
		}

		private void PrintUsageAndExit(int exitCode)
		{
			Console.WriteLine(@"
RavenDB server runner
----------------------------------------
Copyright (C) 2008 - {0} - Hibernating Rhinos
----------------------------------------
Command line options:", DateTime.UtcNow.Year);

			optionSet.WriteOptionDescriptions(Console.Out);
			Console.WriteLine();

			Environment.Exit(exitCode);
		}
	}
}
