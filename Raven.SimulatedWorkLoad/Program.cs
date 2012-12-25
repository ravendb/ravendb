using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.SimulatedWorkLoad.Indexes;
using Raven.SimulatedWorkLoad.Model;

namespace Raven.SimulatedWorkLoad
{
	internal class Program
	{
		private static void Main()
		{
			using (var documentStore = new DocumentStore
			{
				ConnectionStringName = "RavenDB"
			})
			{
				documentStore.Initialize();
				var createIndexes = new CreateIndexes(documentStore);
				var random = new Random(95832);

				var sources = GenerateSources(random, documentStore);

				WriteToDatabase(sources, random, createIndexes);
			}
		}

	

		private static void WriteToDatabase(List<Observing<User>> sources, Random random, CreateIndexes createIndexes)
		{
			int count = 0;
			while (sources.Any(x => x.Completed == false))
			{
				var sizes = sources.Select(_ => random.Next(1, 40)).ToList();
				count++;
				Parallel.ForEach(sources, (observing, state, i) =>
				{
					if (count%3 == 0) // do writes only every third run, ensure more reads than writes (more production)
					{
						observing.Release(sizes[(int) i]);
					}
					createIndexes.DoSomeOtherWork((int)i);
				});

				createIndexes.CreateIndexesSecond();

				Thread.Sleep(random.Next(50, 150));

				if (count%100 == 0)
				{
					createIndexes.Stats();
				}
			}
		}

		private static List<Observing<User>> GenerateSources(Random random, DocumentStore documentStore)
		{
			var sources = Directory.GetFiles(ConfigurationManager.AppSettings["DataPath"], "data*.csv")
			                       .Select(file => new Observing<User>(ReadFromFile(file)))
			                       .ToList();

			foreach (var observable in sources)
			{
				var wait = TimeSpan.FromMilliseconds(random.Next(100, 2400));
				var bufferSize = random.Next(1, 16);
				observable.Buffer(wait, bufferSize)
				          .Subscribe(list =>
				          {
					          using (var session = documentStore.OpenSession())
					          {
						          foreach (var user in list)
						          {
							          session.Store(user);
						          }
						          session.SaveChanges();
					          }
				          });
			}
			return sources;
		}

		private static IEnumerable<User> ReadFromFile(string file)
		{
			using (var data = File.OpenRead(file))
			using (var reader = new StreamReader(data))
			{
				string line;
				while ((line = reader.ReadLine()) != null)
				{
					string[] parts = line.Split('|');
					yield return new User
					{
						First = parts[0],
						Last = parts[1],
						Email = parts[2],
						City = parts[3],
						State = parts[4],
						Zip = parts[5],
						Phone = parts[6],
						StreetAddress = parts[7]
					};
				}
			}
		}
	}

	public class CreateIndexes
	{
		private readonly DocumentStore documentStore;
		int loops = 0;
		bool createdMapIndexes2 = false;
		bool createdMapReduceIndexes2 = false;
			
		public CreateIndexes(DocumentStore documentStore)
		{
			this.documentStore = documentStore;
			new Users_Search().Execute(documentStore);
			new Users_Stats_ByState().Execute(documentStore);
		}		

		public void CreateIndexesSecond()
		{
			if (loops++%10 != 0 || createdMapIndexes2 && createdMapReduceIndexes2)
				return;

			var databaseStatistics = documentStore.DatabaseCommands.GetStatistics();
			if (createdMapIndexes2 == false && databaseStatistics.CountOfDocuments > 25 * 1000)
			{
				new Users_Locations().Execute(documentStore);
				createdMapIndexes2 = true;
			}

			if (createdMapReduceIndexes2 == false && databaseStatistics.CountOfDocuments > 40 * 1000)
			{
				new Users_Stats_ByStateAndcity().Execute(documentStore);
				createdMapReduceIndexes2 = true;
			}
		}

		public void DoSomeOtherWork(int mark)
		{
			var random = new Random();
			using (var session = documentStore.OpenSession())
			{
				switch (mark)
				{
					case 0: // load some docs by ids
						for (int i = 0; i < random.Next(1,4); i++)
						{
							session.Load<User>(random.Next(1, 350000));
						}
						break;
					case 1: // load by id and modify
						var id = random.Next(1, 350000);
						var user1 = session.Load<User>(id);
						if (user1 != null)
						{
							user1.State = user1.State == "TX" ? "CN" : "TX";
						}
						break;
					case 2: // load by id and query
						for (int i = 0; i < mark / 2; i++)
						{
							var user2 = session.Load<User>(random.Next(1, 350000));
							if (user2 != null)
							{
								session.Query<Users_Stats_ByState.Result, Users_Stats_ByState>()
								   .Where(x => x.State == user2.State)
								   .ToList();
							}
						}
						break;
					case 3:
						var user3 = session.Load<User>(random.Next(1, 350000));
						if (createdMapIndexes2 && user3 != null)
						{
							session.Query<User, Users_Locations>()
							       .Where(x => x.City == user3.City)
							       .ToList();
						}
						break;
					case 4:
					var user4 = session.Load<User>(random.Next(1, 350000));
						if (createdMapReduceIndexes2 && user4 != null)
						{
							session.Query<Users_Stats_ByStateAndcity.Result, Users_Stats_ByStateAndcity>()
							       .Where(x => x.City == user4.City && x.State == user4.State)
							       .ToList();
						}
						break;
					case 5:
						goto case 2;
				}

				session.SaveChanges();
			}
		}

		public void Stats()
		{
			var databaseStatistics = documentStore.DatabaseCommands.GetStatistics();
			Console.WriteLine("Wrote {0:#,#} documents, {1} out of {2} indexes stale",
				databaseStatistics.CountOfDocuments,
				databaseStatistics.StaleIndexes.Length,
				databaseStatistics.CountOfIndexes);
		}
	}
}