using System;
using Raven.Abstractions.Data;
using Raven.Tests.Issues;
using Raven.Tests.Notifications;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			for (int i = 0; i < 100; i++)
			{
				Console.Clear();
				Console.WriteLine(i);
				using (var x = new RavenDB_1497())
				{
					x.AfterRestoreOfIncrementalBackupAllIndexesShouldWork();
				}
			}
		}
	}
}