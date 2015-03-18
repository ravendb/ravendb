using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Plugins;
using Raven.Database.Server;
using Raven.Database.Server.Security;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3230 : RavenTest
    {
        private readonly DocumentStore store;

        public RavenDB_3230()
		{
            store = NewRemoteDocumentStore(enableAuthentication:true);
		}
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            Authentication.EnableOnce();
            configuration.AnonymousUserAccessMode = AnonymousUserAccessMode.None;
            configuration.Catalog.Catalogs.Add(new TypeCatalog(typeof(AdminOnlyPutTrigger)));  
        }
		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

        [Fact]
        public void CanPutDocumentUsingBulkInsertWithAdminAuthentication()
        {
            var bulkInsertSize = 20;
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < bulkInsertSize; i++)
                {
                    bulkInsert.Store(new SampleData
                    {
                        Name = "New Data" + i
                    });
                }
            }
            Assert.Equal(bulkInsertSize+1, store.DatabaseCommands.GetStatistics().CountOfDocuments);            
        }
        public class SampleData
        {
            public string Name { get; set; }
        }
        public class AdminOnlyPutTrigger : AbstractPutTrigger
        {
            public override VetoResult AllowPut(string key, RavenJObject document, RavenJObject metadata, TransactionInformation transactionInformation)
            {
                var principal = CurrentOperationContext.User.Value;
                var isAdmin = principal.IsAdministrator(Database.Configuration.AnonymousUserAccessMode) || principal.IsAdministrator(Database);
                return isAdmin ? VetoResult.Allowed : VetoResult.Deny("Only admin may put document into the database");
            }
        }
    }
}
