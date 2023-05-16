// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3760.cs" company="Hibernating Rhinos LTD">
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
    public class RavenDB_3760 : RavenTestBase
    {
        public RavenDB_3760(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Can_use_metadata_in_transform_script(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(master);

                Etl.AddEtl(master, slave, "Users", @"
                        this.Name =  this['@metadata']['User'];
                        loadToUsers(this);");

                using (var session = master.OpenSession())
                {
                    var entity = new User
                    {
                        Name = "foo"
                    };

                    session.Store(entity, "users/1");
                    session.Advanced.GetMetadataFor(entity)["User"] = "bar";

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = slave.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    Assert.Equal("bar", user.Name);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Null_returned_from_script_means_that_document_is_filtered_out(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(master, numOfProcessesToWaitFor: 3);

                Etl.AddEtl(master, slave, "users", @"
if (this.Age % 2 == 0) 
    return;
else 
    this.Name = 'transformed'; 
loadToUsers(this);");

                const int count = 10;

                using (var session = master.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User
                        {
                            Age = i
                        }, "users/" + i);
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = slave.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        var user = session.Load<User>("users/" + i);

                        if (i % 2 == 0)
                        {
                            Assert.Null(user);
                        }
                        else
                        {
                            Assert.Equal("transformed", user.Name);
                        }
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Etl)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void Null_script_means_no_transformation_nor_filtering_within_specified_collection(Options options)
        {
            using (var master = GetDocumentStore(options))
            using (var slave = GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(master, numOfProcessesToWaitFor: 2);

                Etl.AddEtl(master, slave, "Users", null);

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

                    Assert.Equal("Arek", user.Name);

                    var person = session.Load<Person>("people/1");

                    Assert.Null(person);
                }
            }
        }
    }
}
