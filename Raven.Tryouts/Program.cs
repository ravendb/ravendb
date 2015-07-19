using System;
using Raven.Tests.Core;
using Raven.Tests.Core.ChangesApi;

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
				using (var test = new WebsocketsTests())
				{
					test.AreWebsocketsDestroyedAfterGC();
				}			
				Console.WriteLine(i);
			}
		}

	}
}