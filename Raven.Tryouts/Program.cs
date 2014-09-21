using System;
using System.Transactions;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Document.DTC;
using Raven.Tests.Indexes;
using Raven.Tests.Issues;
using Xunit;

namespace Raven.Tryouts
{
    class Program
    {
        private static void Main(string[] args)
        {
			var store = new DocumentStore
			{
				ConnectionStringName = "RavenDB"
			}.Initialize();

			IDocumentSession documentSession = store.OpenSession();
	        var load = documentSession.Advanced.Lazily.Load<dynamic>("users/1");
			var load2 = documentSession.Advanced.Lazily.Load<dynamic>("users/2");

			documentSession.Advanced.Eagerly.ExecuteAllPendingLazyOperations();
        }
    }
}