using System;
using System.Threading;
using Raven.Client.Document;
using Raven.Json.Linq;

public class Program
{
	public static void Main()
	{
		try
		{
			Run();
		}
		catch (Exception ex)
		{
			Console.WriteLine(ex);
		}
	}

	private static void Run()
	{
		using (var store = new DocumentStore { Url = "http://localhost:8080"}.Initialize())
		{
			var id = string.Empty;
			using (var session = store.OpenSession())
			{
				var state = new SessionState { Expires = DateTime.UtcNow.AddDays(30) };
				session.Store(state);
				var ravenJObject = session.Advanced.GetMetadataFor(state);
				ravenJObject["Raven-Expiration-Date"] = new RavenJValue(state.Expires);
				session.SaveChanges();
				id = state.Id;
			}

			var until = DateTime.Now.AddMinutes(10);
			while (DateTime.Now < until)
			{
				using (var session = store.OpenSession())
				{
					var state = session.Load<SessionState>(id);
					if (state == null)
					{
						Console.WriteLine("Document deleted early!");
						return;
					}

					Console.WriteLine("{0}: Found document {1}", DateTime.Now, id);
				}

				Thread.Sleep(TimeSpan.FromSeconds(20));
			}

			Console.WriteLine("No problems");
		}
	}
}

public class SessionState
{
	public string Id { get; set; }
	public DateTime Expires { get; set; }
}