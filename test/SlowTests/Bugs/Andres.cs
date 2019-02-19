using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using System.Linq;
using Xunit;
using FastTests;

namespace SlowTests.Bugs
{
    public class Andres_Indexing : RavenTestBase
    {
        public class Parameter
        {
            public string Name { get; set; }
            public object Value { get; set; }
            public List<string> Tags { get; set; }
        }

        public class Run
        {
            public string Batch { get; set; }
            public long TimeTicks { get; set; }
        }

        public class StepExecutions
        {
            public string Id { get; set; }
            public List<Run> Runs { get; set; }
        }

        public class Attribute
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }

        public class ProcessStepDocument
        {
            public string Id { get; set; }
            public string DeviceId { get; set; }
            public string StepId { get; set; }
            public string StepName { get; set; }
            public string StepExecutionsId { get; set; }
            public string Station { get; set; }
            public string Project { get; set; }
            public string Batch { get; set; }
            public uint RunNumber { get; set; }
            public DateTimeOffset StartTime { get; set; }
            public DateTimeOffset StopTime { get; set; }
            public long StopTimeTicks { get; set; }
            public List<Attribute> Attributes { get; set; }
            public List<Parameter> Parameters { get; set; }
        }

        public class YieldSearch : AbstractIndexCreationTask<ProcessStepDocument, YieldSearch.YieldResult>
        {
            public class YieldResult
            {
                public string WaferId { get; set; }
                public string BinningStep { get; set; }
                public string BatchId { get; set; }
                public int DayNumberSinceMinDate { get; set; }
                public bool LatestRun { get; set; }
                public string ProductCode { get; set; }
                public string PassCount { get; set; }
            }

            public YieldSearch()
            {
                Map = docs =>
                from doc in docs
                where doc.StepName.EndsWith("Binning", StringComparison.Ordinal)
                let tests = LoadDocument<StepExecutions>(doc.StepExecutionsId)
                let waferAttribute = doc.Attributes.FirstOrDefault(a => a.Name == "WaferId")
                from param in doc.Parameters
                where param.Tags.Any(x => x == "Type=ProductCode")
                select new YieldResult
                {
                    WaferId = waferAttribute != null ? waferAttribute.Value : String.Empty,
                    BinningStep = doc.StepId,
                    BatchId = $"{doc.Project}-{doc.Batch}",
                    ProductCode = param.Name,
                    PassCount = param.Value.ToString(),
                    DayNumberSinceMinDate = (int)((DateTimeOffset)doc.StopTime).Subtract(DateTimeOffset.MinValue).TotalDays,
                    LatestRun = tests != null ? tests.Runs.All(r => r.TimeTicks <= doc.StopTimeTicks) : true
                };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        [Fact]
        public void StoreIndex()
        {
            using (var store = GetDocumentStore())
            {
                var index = new YieldSearch();
                index.Execute(store);
            }
        }
    }

}
