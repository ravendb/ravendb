using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Raven.Client.Document;

namespace etobi.EmbeddedTest
{
	internal class Program
	{
		static void Main(string[] args)
		{
			var documentStore = new DocumentStore
			{
				Url = "http://localhost:8080"
			}.Initialize();

			var list = new List<User>();
			using (var session = documentStore.OpenSession())
			{
				for (int i = 0; i < 4098; i++)
				{
					var entity = new User
					{
						Email = "ayende@ayende.com",
						Name = "Ayende Rahien",
						WindowsAccountId = Guid.NewGuid()
					};
					list.Add(entity);
				}
			}
			var serializeObject = JsonConvert.SerializeObject(list);
			File.WriteAllText("test.json",serializeObject);
		}
	}
	public class User
	{
		public string Id { get; set; }
		public string Name { get; set; }
		public Guid WindowsAccountId { get; set; }
		public string Email { get; set; }
	}
}