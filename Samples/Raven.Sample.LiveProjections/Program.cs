//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Sample.LiveProjections
{
	class Program
	{
		static void Main(string[] args)
		{
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				IndexCreation.CreateIndexes(typeof(Users_WithManager).Assembly, store);
				using(var s = store.OpenSession())
				{
					var manager = new User
					{
						Name = "Ayende Rahien"
					};

					s.Store(manager);

					s.Store(new User
					{
						Name = "Oren Eini",
						ManagerId = manager.Id
					});

					s.SaveChanges();
				}

				// manually
				using (var s = store.OpenSession())
				{
					var user = s.Load<User>("users/2");

					Console.WriteLine(user.Name);

					var manager = s.Load<User>(user.ManagerId);
					Console.WriteLine(manager.Name);
				}

				// using includes
				using(var s = store.OpenSession())
				{
					var user = s.Include<User>(x => x.ManagerId)
						.Load("users/2");

					Console.WriteLine(user.Name);

					var manager = s.Load<User>(user.ManagerId);
					Console.WriteLine(manager.Name);
				}

				// using live projections
				using (var s = store.OpenSession())
				{
					var users = s.Query<UserWithManagerProjection, Users_WithManager>()
						.Customize(x=>x.WaitForNonStaleResults())
						.ToList();

					foreach (var user in users)
					{
						Console.WriteLine(user); 
					}
				}


			}
		}
	}

	public class Users_WithManager : AbstractIndexCreationTask<User>
	{
		public Users_WithManager()
		{
			Map = users => from user in users
						   select new {user.Name};
			TransformResults = (database, users) =>
			                   from user in users
			                   let manager = database.Load<User>(user.ManagerId)
			                   select new
			                   {
			                   	ManagerName = manager == null ? "No Manager" : manager.Name,
			                   	user.ManagerId,
			                   	user.Name,
			                   	user.Id
			                   };
		}
	}

	public class UserWithManagerProjection
	{
		public string Id { get; set; }
		public string Name { get; set; }

		public string ManagerId { get; set; }
		public string ManagerName { get; set; }

		public override string ToString()
		{
			return string.Format("Id: {0}, Name: {1}, ManagerId: {2}, ManagerName: {3}", Id, Name, ManagerId, ManagerName);
		}
	}

	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }

		public string ManagerId { get; set; }
	}
}
