//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Raven.Client.Document;
using Raven.Client.Linq;
using System.Linq;

namespace Raven.Sample.SimpleClient
{
	class Program
	{
		static void Main()
		{
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080",
				DefaultDatabase = "Scratch"
			}.Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					var companies = session.Query<Company>()
						.Where(x => x.Name == "Hibernating Rhinos")
						.ToList();

					Console.WriteLine(companies.Count);

					session.SaveChanges();
				}


			}
		}
	}
}
