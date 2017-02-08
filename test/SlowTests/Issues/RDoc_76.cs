// -----------------------------------------------------------------------
//  <copyright file="RDoc_76.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RDoc_76 : RavenNewTestBase
    {
        private class Room
        {
            public string Sth { get; set; }
        }

        private class Kitchen : Room
        {
        }

        private class Bedroom : Room
        {
        }

        private class Guestroom : Bedroom
        {
        }

        private class MasterBedroom : Bedroom
        {
        }

        [Fact]
        public void RegisterIdConventionShouldWorkProperlyForDerivedTypesAsync()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.RegisterAsyncIdConvention<Bedroom>((dbName, r) => new CompletedTask<string>("b/" + r.Sth));
                store.Conventions.RegisterAsyncIdConvention<Guestroom>((dbName, r) => new CompletedTask<string>("gr/" + r.Sth));
                store.Conventions.RegisterAsyncIdConvention<Room>((dbName, r) => new CompletedTask<string>("r/" + r.Sth));
                store.Conventions.RegisterAsyncIdConvention<Kitchen>((dbName, r) => new CompletedTask<string>("k/" + r.Sth));
                store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => new CompletedTask<string>("mb/" + r.Sth));

                using (var session = store.OpenSession())
                {
                    session.Store(new MasterBedroom { Sth = "1" });
                    session.Store(new Guestroom { Sth = "2" });
                    session.Store(new Bedroom { Sth = "3" });
                    session.Store(new Room { Sth = "4" });
                    session.Store(new Kitchen { Sth = "5" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var mbs = session.Query<MasterBedroom>()
                                     .Customize(x => x.WaitForNonStaleResults())
                                     .ToList();
                }

                using (var commands = store.Commands())
                {
                    Assert.NotNull(commands.Get("mb/1"));
                    Assert.NotNull(commands.Get("gr/2"));
                    Assert.NotNull(commands.Get("b/3"));
                    Assert.NotNull(commands.Get("r/4"));
                    Assert.NotNull(commands.Get("k/5"));
                }
            }
        }

        [Fact]
        public async Task RegisteringConventionForSameTypeShouldOverrideOldOneAsync()
        {
            using (var store = GetDocumentStore())
            {
                store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => new CompletedTask<string>("a/" + r.Sth));
                store.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => new CompletedTask<string>("mb/" + r.Sth));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new MasterBedroom { Sth = "1" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var mbs = await session.Query<MasterBedroom>()
                                           .Customize(x => x.WaitForNonStaleResults())
                                           .ToListAsync();
                }

                using (var commands = store.Commands())
                {
                    Assert.Null(commands.Get("a/1"));
                    Assert.NotNull(commands.Get("mb/1"));
                }
            }
        }
    }
}
