// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3639.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_3639 : RavenTestBase
    {
        public RavenDB_3639(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Docs_are_transformed_according_to_provided_collection_specific_scripts(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(master, numOfProcessesToWaitFor: 2);

                Etl.AddEtl(master, slave, "users",
                    @"this.Name = 'patched ' + this.Name;
                      loadToUsers(this)");

                using (var session = master.OpenSession())
                {
                    session.Store(new Person
                    {
                        Name = "Arek"
                    }, "people/1");

                    session.Store(new User
                    {
                        Name = "Arek"
                    }, "users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = slave.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("patched Arek", user.Name);

                    var person = session.Load<Person>("people/1");

                    Assert.Null(person);
                }
            }
        }
    }
}
