using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Raven.Client.Document;

namespace Raven.Sample.ReplicationComplex
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8079",
				Conventions =
					{
						FailoverBehavior = FailoverBehavior.ReadFromAllServers
					}
				
			}.Initialize())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new User { Name = "Ayende" }, "users/ayende");
					session.SaveChanges();
				}

				Console.WriteLine("Waiting");
				Console.ReadLine();

				for (int i = 0; i < 12; i++)
				{
					using (var session = store.OpenSession())
					{
						session.Load<User>("users/ayende");
					}
				}
			}
		}

		
	}

	public class User
	{
		public string Name { get; set; }
	}
}
