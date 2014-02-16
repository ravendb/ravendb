using System;
using Raven.Tests.Issues;
using Xunit;

namespace Raven.Tryouts
{
	class Program
	{
		private static void Main(string[] args)
		{
		    for (int i = 0; i < 100; i++)
		    {
		        Console.WriteLine(i);
                using (var x = new RavenDB_1041())
                {
                    x.CanWaitForReplication().Wait();
                }
		    }
		}
	}
}