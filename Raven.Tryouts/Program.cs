using System;
using System.Diagnostics;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Impl;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Issues;

namespace Raven.Tryouts
{
	internal class Program
	{
		[STAThread]
		private static void Main()
		{
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				new SlowIndex.Orders_TotalByCustomerFor30Days().Execute(store);
				SlowIndex.GenerateRandomOrders(store, 100, DateTime.Today.AddMonths(-3), DateTime.Today);
				
			}
		}
	}
}