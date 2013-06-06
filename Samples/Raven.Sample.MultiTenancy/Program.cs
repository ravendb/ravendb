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
using Raven.Client.Extensions;

namespace Raven.Sample.MultiTenancy
{
	class Program
	{
		static void Main(string[] args)
		{
			using(var store = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				store.DatabaseCommands.EnsureDatabaseExists("Brisbane");

				store.DatabaseCommands.EnsureDatabaseExists("Melbroune");
				store.DatabaseCommands.EnsureDatabaseExists("Sidney");

				using (var documentSession = store.OpenSession("Brisbane"))
				{
					documentSession.Store(new { Name = "Ayende"});
					documentSession.SaveChanges();
				}
			}
		}
	}
}
