// -----------------------------------------------------------------------
//  <copyright file="RDoc_76.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Issues
{
    public class RDoc_76 : RavenTestBase
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
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.RegisterAsyncIdConvention<Bedroom>((dbName, r) => Task.FromResult("b/" + r.Sth));
                    s.Conventions.RegisterAsyncIdConvention<Guestroom>((dbName, r) => Task.FromResult("gr/" + r.Sth));
                    s.Conventions.RegisterAsyncIdConvention<Room>((dbName, r) => Task.FromResult("r/" + r.Sth));
                    s.Conventions.RegisterAsyncIdConvention<Kitchen>((dbName, r) => Task.FromResult("k/" + r.Sth));
                    s.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => Task.FromResult("mb/" + r.Sth));
                }
            }))
            {
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
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => Task.FromResult("a/" + r.Sth));
                    s.Conventions.RegisterAsyncIdConvention<MasterBedroom>((dbName, r) => Task.FromResult("mb/" + r.Sth));
                }
            }))
            {
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
