using System;
using Raven.Tests.Core.ChangesApi;
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
				using (var test = new RavenDB_3570())
				{
				}			
				Console.WriteLine(i);
			}
		}

	}
}