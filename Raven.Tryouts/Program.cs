using System;
using System.Collections.Concurrent;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Util;
using Raven.Json.Linq;
using Raven.Tests.Bugs;
using Raven.Tests.Notifications;
using Xunit;

public class Program
{
	public static void Main()
	{
		var q =
			@"docs.Orders.Select(order => new {order = order, lastPayment = order.Payments.LastOrDefault()}).Select(__h__TransparentIdentifier0 => new {Query = new System.Object []{__h__TransparentIdentifier0.order.FirstName, __h__TransparentIdentifier0.order.LastName, __h__TransparentIdentifier0.order.OrderNumber, __h__TransparentIdentifier0.order.Email, __h__TransparentIdentifier0.order.Email.Split(new System.Char []{'@'}), __h__TransparentIdentifier0.order.CompanyName, __h__TransparentIdentifier0.order.Payments.Select(payment => payment.PaymentIdentifier), __h__TransparentIdentifier0.order.LicenseIds}, LastPaymentDate = __h__TransparentIdentifier0.lastPayment == null ? __h__TransparentIdentifier0.order.OrderedAt : __h__TransparentIdentifier0.lastPayment.At})";

		Console.WriteLine(JSBeautify.Apply(q.Replace("__h__TransparentIdentifier", "this")));

		return;
		for (int i = 0; i < 1000; i++)
		{
			Console.WriteLine(i);

			using (var x = new Embedded())
			{
				x.CanGetNotificationAboutDocumentIndexUpdate();
			}

			GC.Collect(2);
			GC.WaitForPendingFinalizers();


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
