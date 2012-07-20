using System;
using System.Collections.Concurrent;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Notifications;
using Xunit;

public class Program
{
	public static void Main()
	{
		for (int i = 0; i < 100; i++)
		{
			Console.WriteLine(i);

			using (var x = new WithIIS())
			{
				x.CheckNotificationInIIS();
			}

			GC.Collect(2);
			GC.WaitForPendingFinalizers();

			//using (var x = new ClientServer())
			//{
			//    x.CanGetNotificationAboutDocumentDelete();
			//}
			//GC.Collect(2);
			//GC.WaitForPendingFinalizers();


			//using (var x = new NotificationOnWrongDatabase())
			//{
			//    x.ShouldNotCrashServer();
			//}
			//GC.Collect(2);
			//GC.WaitForPendingFinalizers();
			//using (var x = new ClientServer())
			//{
			//    x.CanGetNotificationAboutDocumentIndexUpdate();
			//}



			//GC.Collect(2);
			//GC.WaitForPendingFinalizers();
		}
	}
}