using Raven.Abstractions.Data;
using Raven.Tests.Core.Utils.Entities;
using System;
using System.Linq;
using System.Text;
using Xunit;

namespace Raven.Tests.Core.ChangesApi
{
    public class Subscribing : RavenCoreTestBase
    {
        [Fact]
        public void CanSubscribeToDocumentChanges()
        {
            using (var store = GetDocumentStore())
            {
                var output = "";

                store.Changes()
                    .ForAllDocuments()
                    .Subscribe(change =>
                    {
                        output = "passed_foralldocuments";
                    });

                store.Changes()
                    .ForDocumentsStartingWith("companies")
                    .Subscribe(change => 
                    {
                        output = "passed_forfordocumentsstartingwith";
                    });

                store.Changes()
                    .ForDocument("companies/1")
                    .Subscribe(change => 
                    {
                        if (change.Type == DocumentChangeTypes.Delete)
                        {
                            output = "passed_fordocumentdelete";
                        }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new User 
                    {
                        Id = "users/1"
                    });
                    session.SaveChanges();
                    Assert.Equal("passed_foralldocuments", output);

                    session.Store(new Company
                    {
                        Id = "companies/1"
                    });
                    session.SaveChanges();
                    Assert.Equal("passed_forfordocumentsstartingwith", output);

                    session.Delete("companies/1");
                    session.SaveChanges();
                    Assert.Equal("passed_fordocumentdelete", output);
                }
            }
        }
    }
}
