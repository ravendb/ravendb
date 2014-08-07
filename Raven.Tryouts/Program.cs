using System;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			for (int i = 0; i < 100; i++)
			{
				Console.WriteLine(i);
				using (var x = new RavenDB_1497())
				{
					x.AfterRestoreOfIncrementalBackupAllIndexesShouldWork();
				}
			}
		}
	}
}