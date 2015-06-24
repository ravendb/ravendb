using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Abstractions.Commands;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Json.Linq;
using Raven.Tests.MailingList;

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
			
		}

	}
}