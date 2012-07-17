using System;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Notifications;

public class Program
{
	public static void Main()
	{
		for (int i = 0; i < 1999; i++)
		{
			Console.WriteLine(i);
			using(var x = new ClientServer())
				x.CanGetNotificationAboutDocumentPut();
		}
	}
}