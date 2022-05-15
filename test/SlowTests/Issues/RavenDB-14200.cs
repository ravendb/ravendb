using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Linq;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14200 : RavenTestBase
    {
        public RavenDB_14200(ITestOutputHelper output) : base(output)
        {
        }

        private class Batch
        {
            public Job Job { get; set; }
        }

        private class BatchDto
        {
            public string DeviceName { get; set; }

            public string UnitName { get; set; }

            public string BatchName { get; set; }
        }

        private class Job
        {
            public Details Details { get; set; }

            public Equipment Equipment { get; set; }
        }

        private class Details
        {
            public Details()
            {
                Media = new List<Media>();
            }

            public ICollection<Media> Media { get; }
        }

        private class Equipment
        {
            public Device Device { get; set; }
        }

        private class Media
        {
            public string Name { get; set; }
        }

        private class Device
        {
            public string Name { get; set; }

            public Unit Unit { get; set; }

            public Guid DeviceId { get; set; }

            public bool IsBioUnitSelected => DeviceId != Guid.Empty
                                             && Unit != null
                                             && Unit.UnitId != Guid.Empty;
        }

        private class Unit
        {
            public string Name { get; set; }

            public Guid UnitId { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseStringEmptyInJsProjection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.Store(new User
                    {
                        Name = "jerry"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Query<User>().Select(u => new
                    {
                        Name = u.Name ?? string.Empty
                    }).ToList();

                    Assert.Equal(2, query.Count);

                    Assert.Equal(string.Empty, query[0].Name);
                    Assert.Equal("jerry", query[1].Name);

                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseStringEmptyInJsProjection2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Batch
                    {
                        Job = new Job
                        {
                            Details = new Details { Media = { new Media { Name = "Media" } } },
                            Equipment = new Equipment
                            {
                                Device = new Device
                                {
                                    Name = "Device",
                                    DeviceId = Guid.NewGuid(),
                                    Unit = new Unit { Name = "Unit", UnitId = Guid.NewGuid() }
                                }
                            }
                        }
                    });

                    session.SaveChanges();

                    var query = (from batch in session.Query<Batch>().Customize(x => x.WaitForNonStaleResults())
                            select new BatchDto
                            {
                                DeviceName = batch.Job.Equipment.Device != null
                                    ? batch.Job.Equipment.Device.Name
                                    : string.Empty,
                                UnitName = batch.Job.Equipment.Device != null
                                           && batch.Job.Equipment.Device.IsBioUnitSelected
                                    ? batch.Job.Equipment.Device.Unit.Name
                                    : string.Empty,
                                BatchName = string.Join(", ", batch.Job.Details.Media.Select(m => m.Name))
                            })
                        .ToList();
                }
            }
        }
    }
}
