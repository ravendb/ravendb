using System.Collections.Generic;

using Raven.Tests.Server.Runner.Data;

namespace Raven.Tests.Server.Runner
{
	using System;

	using NDesk.Options;

	public class Program
	{
		private readonly OptionSet optionSet;

		private int port = 8585;

		private Program()
		{
			Context.Clear();
			AppDomain.CurrentDomain.ProcessExit += (sender, eventArgs) => Context.Clear();

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

		private void Parse(IEnumerable<string> args)
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

			using (new ServerRunner(port))
			{
				Console.WriteLine("Press Enter to Exit");
				Console.ReadLine();
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
