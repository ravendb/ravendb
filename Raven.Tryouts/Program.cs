using System;
using System.Threading;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	public class Customer
	{
		public string Region;
		public string Id;
	}

	public class Invoice
	{
		public string Customer;
	}

	public class Program
	{
		private static void Main()
		{
			for (int i = 0; i < 1000; i++)
			{
				using (var test = new RavenDB_3629())
				{
					test.Referenced_files_should_be_replicatedB();
				}			
				Console.WriteLine(i);
				Thread.Sleep(100);
			}
		}

	}
}