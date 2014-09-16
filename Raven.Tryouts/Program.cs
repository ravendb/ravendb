using System;
using Raven.Abstractions.Data;
using Raven.Tests.Notifications;

namespace Raven.Tryouts
{
    public class Program
	{
		private static void Main(string[] args)
		{
			for (int i = 0; i < 10; i++)
			{
				Console.WriteLine(i);
				using (var x = new Embedded())
				{
					x.CanGetNotificationAboutDocumentPut();
				}
			}
		}
	}
}