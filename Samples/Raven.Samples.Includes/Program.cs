//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;

namespace Raven.Samples.Includes
{
	internal class Program
	{
		private static void Main()
		{
			using (var documentStore = new DocumentStore {Url = "http://localhost:8080"})
			{
				documentStore.Initialize();
			
				using (IDocumentSession session = documentStore.OpenSession())
				{
					var company = new Company
					{
						Name = "Hibernating Rhinos",
					};
					session.Store(company);
					session.Store(new Company
					{
						Name = "Rampaging Rhinos",
						ParentCompanyId = company.Id
					});
					session.SaveChanges();
				}
				Console.ReadLine();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var company = session.Load<Company>("companies/2");

					var parent = session.Load<Company>(company.ParentCompanyId);

					Console.WriteLine(company.Name);
					Console.WriteLine(parent.Name);
				}

				Console.ReadLine();

				using (IDocumentSession session = documentStore.OpenSession())
				{
					var company = session.Include<Company>(x => x.ParentCompanyId)
						.Load("companies/2");

					var parent = session.Load<Company>(company.ParentCompanyId);

					Console.WriteLine(company.Name);
					Console.WriteLine(parent.Name);
				}
			}
		}
	}
}
