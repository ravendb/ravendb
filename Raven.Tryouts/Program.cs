using System;
using System.IO;
using System.Net;
using log4net;
using log4net.Appender;
using log4net.Config;
using log4net.Layout;
using Raven.Client.Document;
using Raven.Scenarios;
using Raven.Tests.Indexes;
using Raven.Tests.Triggers;

namespace Raven.Tryouts
{
	internal class Program
	{
		private static void Main()
		{
		   using(var store = new DocumentStore
		   {
			   ConnectionStringName = "db"
		   }.Initialize())
		   {
		   	using(var session = store.OpenSession())
		   	{
		   		session.Store(new {Ayende = "Rahien"});
				session.SaveChanges();
		   	}
		   }
		}
	}
}
