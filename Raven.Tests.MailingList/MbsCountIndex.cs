using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class MbsCountIndex : RavenTestBase
    {
        [Fact]
        public void TypeIssue()
        {
            var now = DateTime.UtcNow;
            using (EmbeddableDocumentStore store = NewDocumentStore(configureStore: ConfigureTestStore))
            {
                new MBS_Counts2().Execute(store);
                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new MyMbsStatus {
                        Id = "AppUser/4",
                        glucose = 154,
                        hba1c = 11.1,
                        systolic = 109,
                        diastolic = 75,
                        hdl = 64,
                        waist = 42,
                        trig = 136,
                        status = "Missing",
                        When = now
                    });

                    session.SaveChanges();
                }
                WaitForIndexing(store);
                WaitForUserToContinueTheTest(store);
                using (IDocumentSession session = store.OpenSession())
                {
                    var records = session.Query<MyMbsStatus, MBS_Counts2>().Where(x => x.When == now).ToList();
                    Assert.NotEqual(records.Count,0);
                }
            }
        }
        public void ConfigureTestStore(EmbeddableDocumentStore documentStore)
        {
            documentStore.Configuration.Storage.Voron.AllowOn32Bits = true;
        }


        public class MBS_Counts2 : AbstractIndexCreationTask<MyMbsStatus, MBS_Counts2.Result>
        {
            public class Result
            {
                public string UserId { get; set; }
                public int Year { get; set; }
                public DateTime When { get; set; }
                public GenderType Gender { get; set; }
                public int? WaistRisk { get; set; }
                public int? HdlRisk { get; set; }
                public int? TrigRisk { get; set; }
                public int? BpRisk { get; set; }
                public int? SugarRisk { get; set; }
            }


            public MBS_Counts2()
            {
                Map = docs =>
                              from mbsStatus in docs
                              select new Result
                              {
                                  UserId = mbsStatus.Id,
                                  Year = mbsStatus.When.Value.Year,
                                  When = mbsStatus.When.Value,
                                  Gender = mbsStatus.Gender,
                                  WaistRisk = ((mbsStatus.Gender == GenderType.Female && mbsStatus.waist.Value > 35.0 || mbsStatus.Gender == GenderType.Male && mbsStatus.waist.Value == 40.0)) ? 1 : 0,
                                  HdlRisk = ((mbsStatus.Gender == GenderType.Female && mbsStatus.hdl.Value < 50.0 || mbsStatus.Gender == GenderType.Male && mbsStatus.hdl.Value == 40.0)) ? 1 : 0,
                                  TrigRisk = mbsStatus.trig.Value == 150.0 ? 1 : 0,
                                  BpRisk = (mbsStatus.systolic.Value == 130.0 || mbsStatus.diastolic.Value >= 85.0) ? 1 : 0,
                                  SugarRisk = mbsStatus.hba1c.HasValue ? mbsStatus.hba1c.Value == 5.7 ? 1 : 0 : mbsStatus.glucose.Value == 100.0 ? 1 : 0,
                              };
                Reduce = docs => from doc in docs
                                 group doc by new { User = doc.UserId, doc.Year, doc.When } into g
                                 select new Result
                                 {
                                     UserId = g.Key.User,
                                     Year = g.Key.Year,
                                     When = g.Last().When,
                                     Gender = g.Last().Gender,
                                     WaistRisk = g.Where(x => x.WaistRisk != null).Last().WaistRisk,
                                     HdlRisk = g.Where(x => x.HdlRisk != null).Last().HdlRisk,
                                     TrigRisk = g.Where(x => x.TrigRisk != null).Last().TrigRisk,
                                     BpRisk = g.Where(x => x.BpRisk != null).Last().BpRisk,
                                     SugarRisk = g.Where(x => x.SugarRisk != null).Last().SugarRisk,
                                 };
            }
        }
        public class MyMbsStatus
        {
            public MyMbsStatus()
            {
            }
            public string Id { get; set; }
            public GenderType Gender { get; set; }
            public double? glucose { get; set; }
            public double? hba1c { get; set; }
            public double? systolic { get; set; }
            public double? diastolic { get; set; }
            public double? hdl { get; set; }
            public double? waist { get; set; }
            public double? trig { get; set; }
            public string status { get; set; }
            public DateTime? When { get; set; }
        }

        public enum GenderType
        {
            Male,
            Female
        }
    }

}
