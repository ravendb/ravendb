using System;
using System.Net;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Microsoft.Silverlight.Testing;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Raven.Client.Document;

namespace Raven.Client.Silverlight.Tests
{
	[TestClass]
	public class Tests
	{
		public class Item
		{
			public string Name { get; set; }
			public string Id { get; set; }
		}

		[TestMethod]
		public void Can_put_and_load_documet()
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				using (var session = store.OpenAsyncSession())
				{
					session.Store(new Item {Name = "Ayende", Id = "abc"});
					session.SaveChangesAsync().Wait();
				}

				using (var session = store.OpenAsyncSession())
				{
					var loadAsync = session.LoadAsync<dynamic>("abc");

					Assert.AreEqual("Ayende", loadAsync.Result.Name);
				}
			}
		}
	}
}