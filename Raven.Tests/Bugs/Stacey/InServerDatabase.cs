using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Imports.Newtonsoft.Json;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Abstractions.Extensions;

namespace Raven.Tests.Bugs.Stacey
{
	public abstract class InServerDatabase : IDisposable
	{
		protected readonly EmbeddableDocumentStore Embedded;
		private readonly List<IDocumentStore> ListToDispose = new List<IDocumentStore>();

		protected InServerDatabase()
		{
			Embedded = new EmbeddableDocumentStore()
			           	{
			           		RunInMemory = true,
			           		UseEmbeddedHttpServer = true,
			           	};
			Embedded.Initialize();
		}

		protected IDocumentStore DocumentStore()
		{
			var store = new DocumentStore
			       	{
			       		Url = Embedded.Configuration.ServerUrl,
			       		Conventions =
			       			{
			       				CustomizeJsonSerializer = serializer => serializer.TypeNameHandling = TypeNameHandling.All

			       			}
			       	}.Initialize();
			ListToDispose.Add(store);
			return store;
		}

		public void Dispose()
		{
			ListToDispose
				.Where(documentStore => documentStore != null)
				.ForEach(store => store.Dispose());			

			if (Embedded != null)
				Embedded.Dispose();
		}
	}
}