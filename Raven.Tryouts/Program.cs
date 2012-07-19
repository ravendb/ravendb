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
		using (var store = new DocumentStore
		                   	{
								Url = "http://localhost.fiddler:9001"
		                   	}.Initialize())
		{
			var list = new BlockingCollection<DocumentChangeNotification>();
			var taskObservable = store.Changes();
			taskObservable.Task.Wait();
			var observableWithTask = taskObservable.DocumentSubscription("items/1");
			observableWithTask.Task.Wait();

			observableWithTask.Subscribe(list.Add);

			using (var session = store.OpenSession())
			{
				session.Store(new Item(), "items/1");
				session.SaveChanges();
			}

			DocumentChangeNotification documentChangeNotification;
			if(list.TryTake(out documentChangeNotification, TimeSpan.FromSeconds(5)))
			{
				Assert.Equal("items/1", documentChangeNotification.Name);
				Assert.Equal(documentChangeNotification.Type, DocumentChangeTypes.Put);	
			}
			else
			{
				Console.WriteLine("NOT FOUND");
			}

			
		}
	}
}