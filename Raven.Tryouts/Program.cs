//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Storage.Esent;

namespace etobi.MemLeakTest
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
				for (int i = 0; i < 5000; i++)
				{
					using (var s = store.OpenSession())
					{
						for (int j = 0; j < 128; j++)
						{
							s.Store(new { Id = "item/" + i + "/" + j, Language = new { Name = "English" } });
						}

						s.SaveChanges();
					}
				}
				using (var s = store.OpenSession())
				{
					var objects = s.Advanced.LuceneQuery<object>() 
						.WhereEquals("Language.Name", "English")
						.ToArray();

					Console.WriteLine(objects.Length);
				}
			}
		}

	}
}
