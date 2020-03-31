//-----------------------------------------------------------------------
// <copyright file="ProjectingDates.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class ProjectingDates : RavenTestBase
    {
        public ProjectingDates(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSaveCachedVery()
        {
            using(var store = GetDocumentStore())
            {
                var index = new IndexDefinitionBuilder<Registration, Registration>("Regs")
                {
                    Map = regs => from reg in regs
                        select new {reg.RegisteredAt},
                    Stores = {{x => x.RegisteredAt, FieldStorage.Yes}}
                }.ToIndexDefinition(new DocumentConventions());
                store.Maintenance.Send(new PutIndexesOperation( index ));
               
                using(var session = store.OpenSession())
                {
                    session.Store(new Registration
                    {
                        RegisteredAt = new DateTime(2010, 1, 1),
                        Name = "ayende"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var registration = session.Advanced.DocumentQuery<Registration>("Regs")
                        .SelectFields<Registration>("RegisteredAt")
                        .WaitForNonStaleResults()
                        .First();

                    Assert.Equal(new DateTime(2010, 1, 1, 0, 0, 0, DateTimeKind.Local), registration.RegisteredAt);
                    Assert.NotNull(registration.Id);
                    Assert.Null(registration.Name);
                }

                using (var session = store.OpenSession())
                {
                    var registration = session.Advanced.DocumentQuery<Registration>("Regs")
                        .SelectFields<Registration>("RegisteredAt", "Id")
                        .WaitForNonStaleResults()
                        .First();

                    Assert.Equal(new DateTime(2010, 1, 1,0,0,0,DateTimeKind.Local), registration.RegisteredAt);
                    Assert.NotNull(registration.Id);
                    Assert.Null(registration.Name);
                }
            }
        }

        public class Registration
        {
            public string Id { get; set; }

            public DateTime RegisteredAt { get; set; }

            public string Name { get; set; }
        }
    }
}
