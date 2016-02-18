using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class ThorMigrationIssues : RavenTestBase
    {
        private readonly IDocumentStore _docStore;

        public ThorMigrationIssues()
        {
            _docStore = NewRemoteDocumentStore(fiddler: true);
            _docStore.Conventions.DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite;

            var dut = new OrgUnit
            {
                Id = "OrgUnit/1",
                OrgUnitName = "School1",
                Rooms = new List<OrgUnit.Room>
                {
                    new OrgUnit.Room
                    {
                        RoomId = "1",
                        RoomName = "ClassRoom1"
                    },
                    new OrgUnit.Room
                    {
                        RoomId = "2",
                        RoomName = "DarkRoom1"
                    },
                    new OrgUnit.Room
                    {
                        RoomId = "3",
                        RoomName = "Gym1"
                    }
                }
            };

            using (var session = _docStore.OpenSession())
            {

                new UnitRoomsIndexStoreAll().Execute(_docStore);
                new UnitRoomsIndexStoreLess().Execute(_docStore);
                new UnitRoomsTransformer().Execute(_docStore);

                session.Store(dut);
                session.SaveChanges();

                // Ensure transaction completed and index is non-stale
                session.Advanced.AllowNonAuthoritativeInformation = false;

                session.Load<OrgUnit>("OrgUnit/1");
            }
        }

        public override void Dispose()
        {
            base.Dispose();

            if (_docStore.WasDisposed == false)
                _docStore.Dispose();
        }

        [Fact]
        public void CanPreventMultipleResultsFromFanoutIndexStoreAll()
        {
            using (var session = _docStore.OpenSession())
            {
                var query = session.Query<UnitRoomsIndexResult, UnitRoomsIndexStoreAll>()
                    .Customize(x => x.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(false))
                    .TransformWith<UnitRoomsTransformer, UnitRoomsIndexResult>();

                var results = query.ToList();

                Assert.Equal(1, results.Count);
            }
        }

        [Fact]
        public void CanTransferMultipleResultsFromFanoutIndexStoreAll()
        {
            using (var session = _docStore.OpenSession())
            {
                var query = session.Query<UnitRoomsIndexResult, UnitRoomsIndexStoreAll>()
                    .Customize(x => x.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(true))
                    .TransformWith<UnitRoomsTransformer, UnitRoomsIndexResult>();

                var results = query.ToList();

                Assert.Equal(3, results.Count);
            }
        }

        [Fact]
        public void CanPreventMultipleResultsFromFanoutIndexStoreLess()
        {
            using (var session = _docStore.OpenSession())
            {
                var query = session.Query<UnitRoomsIndexResult, UnitRoomsIndexStoreLess>()
                    .Customize(x => x.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(false))
                    .TransformWith<UnitRoomsTransformer, UnitRoomsIndexResult>();

                var results = query.ToList();

                Assert.Equal(1, results.Count);
            }
        }

        [Fact]
        public void CanTransferMultipleResultsFromFanoutIndexStoreLess()
        {
            using (var session = _docStore.OpenSession())
            {
                var query = session.Query<UnitRoomsIndexResult, UnitRoomsIndexStoreLess>()
                    .Customize(x => x.SetAllowMultipleIndexEntriesForSameDocumentToResultTransformer(true))
                    .TransformWith<UnitRoomsTransformer, UnitRoomsIndexResult>();

                var results = query.ToList();

                Assert.Equal(3, results.Count);
            }
        }
    }

    public class UnitRoomsTransformer : AbstractTransformerCreationTask<UnitRoomsIndexResult>
    {
        public UnitRoomsTransformer()
        {
            TransformResults = result => from item in result
                                         select new
                                         {
                                             item.OrgUnitId,
                                             item.OrgUnitName,
                                             item.RoomId,
                                             item.RoomName
                                         };
        }
    }

    public class UnitRoomsIndexResult
    {
        public string OrgUnitId { get; set; }
        public string OrgUnitName { get; set; }

        public string RoomId { get; set; }
        public string RoomName { get; set; }
    }

    public class UnitRoomsIndexStoreAll : AbstractMultiMapIndexCreationTask<UnitRoomsIndexResult>
    {
        public UnitRoomsIndexStoreAll()
        {
            AddMap<OrgUnit>(items => from orgunit in items
                                     from room in orgunit.Rooms
                                     select new
                                     {
                                         OrgUnitId = orgunit.Id,
                                         OrgUnitName = orgunit.OrgUnitName,

                                         RoomId = room.RoomId,
                                         RoomName = room.RoomName
                                     });
            StoreAllFields(FieldStorage.Yes);
        }
    }

    public class UnitRoomsIndexStoreLess : AbstractMultiMapIndexCreationTask<UnitRoomsIndexResult>
    {
        public UnitRoomsIndexStoreLess()
        {
            AddMap<OrgUnit>(items => from orgunit in items
                                     from room in orgunit.Rooms
                                     select new
                                     {
                                         OrgUnitId = orgunit.Id,
                                         OrgUnitName = orgunit.OrgUnitName,

                                         RoomId = room.RoomId,
                                         RoomName = room.RoomName
                                     });
            Stores.Add(f => f.OrgUnitId, FieldStorage.Yes);
            Stores.Add(f => f.RoomName, FieldStorage.Yes);
        }
    }

    public class OrgUnit
    {
        public string Id { get; set; }
        public string OrgUnitName { get; set; }

        public List<Room> Rooms { get; set; }

        public class Room
        {
            public string RoomId { get; set; }
            public string RoomName { get; set; }
        }
    }
}


