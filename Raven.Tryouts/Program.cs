using System;
using Raven.Client.Document;

namespace Raven.Tryouts
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var store = new DocumentStore
			{
				Url = "http://testrunner-pc:8080",
				DefaultDatabase = "ReplicationA",
				ApiKey = "Replication/5XA4ggEdJCG19GCVjihCOX",
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
				}
			}.Initialize())
			{
				while (true)
				{
					using (var session = store.OpenSession())
					{
						session.Store(new User());
						session.SaveChanges();
					}

					Console.WriteLine("Wrote @ " + DateTime.Now);
					Console.ReadLine();
				}
			}
		} 
	}

	public class User
	{
	}

}