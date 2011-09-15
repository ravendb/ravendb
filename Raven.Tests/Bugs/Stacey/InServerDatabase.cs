using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;

namespace Raven.Tests.Bugs.Stacey
{
	public abstract class InServerDatabase : IDisposable {
		protected EmbeddableDocumentStore Embedded;
		private List<IDocumentStore> ListToDispose = new List<IDocumentStore>();
		protected InServerDatabase()
		{
			Embedded = new EmbeddableDocumentStore() {
				RunInMemory = true,
				UseEmbeddedHttpServer = true,
			};
			Embedded.Initialize();
		}

		protected IDocumentStore DocumentStore()
		{
			return new DocumentStore
			{
				Url = Embedded.Configuration.ServerUrl,
				Conventions =
					{
						CustomizeJsonSerializer = serializer =>
						                          serializer.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All

					}
			}.Initialize();
		}

		public void Dispose() {
			foreach (var documentStore in ListToDispose)
			{
				documentStore.Dispose();
			} 
			
			if (Embedded != null)
				Embedded.Dispose();

			
		}
	}
}