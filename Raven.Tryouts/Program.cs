using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Shard;
using Raven.Tests.Faceted;
using System.Linq;
using Raven.Tests.MailingList;

internal class Program
{
	private static void Main(string[] args)
	{
		for (int i = 0; i < 100; i++)
		{
			Console.Clear();
			Console.WriteLine(i);
			using(var x= new FailingChangesApiTests())
			{
				x.Should_get_independent_notification_subscriptions();
			}
		}
	}
}
