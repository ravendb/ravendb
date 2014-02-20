using System;
using System.Globalization;
using Xunit;
using Raven.Tests.Bugs.DTC;
using Raven.Tests.Bundles.Replication;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main(string[] args)
		{
			CultureInfo.DefaultThreadCurrentCulture = new CultureInfo("pl-PL");
			CultureInfo.DefaultThreadCurrentUICulture = new CultureInfo("pl-PL");
			for (int i = 0; i < 1000; i++)
			{
				Console.Clear();
				Console.WriteLine(i);

                Environment.SetEnvironmentVariable("run", i.ToString("000"));
				using (var x = new FailoverDisabled())
				{
					x.CanDisableFailoverByDisablingDestination();
				}
			}
			
		}
	}
}