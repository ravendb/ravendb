using System;
using System.Collections.Generic;
using System.IO;
using Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Client.Embedded;

namespace etobi.EmbeddedTest
{
	internal class Program
	{
		static void Main(string[] args)
		{
			var docStore = new EmbeddableDocumentStore
			{
				UseEmbeddedHttpServer = true,
				DataDirectory = "~\\Data",

			}.Initialize();

			Console.ReadLine();
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