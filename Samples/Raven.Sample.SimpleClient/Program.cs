//-----------------------------------------------------------------------
// <copyright file="Program.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Client.Document;

namespace Raven.Sample.SimpleClient
{
	class Program
	{
		static void Main()
		{
			using (var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize())
			{
				using (var session = documentStore.OpenSession())
				{
					session.SaveChanges();
				}
			}
		}
	}
}
