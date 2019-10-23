using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB8983 : RavenTestBase
    {
        public RavenDB8983(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSaveLarge()
        {
            using (var store = GetDocumentStore())
            {
                new LargeIndex().Execute(store);
            }
        }

        public class LargeIndex : AbstractIndexCreationTask<Doc>
        {
            public LargeIndex()
            {
                Map = docs =>
                    from doc in docs
                    select new
                    {
                        Id = doc.Id,
                        IntVal1 = doc.IntVals["LongValueNameForNumericValueIntVal1"],
                        IntVal1Category = doc.IntVals["LongValueNameForNumericValueIntVal1"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal1"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal1"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal1"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal2 = doc.IntVals["LongValueNameForNumericValueIntVal2"],
                        IntVal2Category = doc.IntVals["LongValueNameForNumericValueIntVal2"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal2"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal2"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal2"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal3 = doc.IntVals["LongValueNameForNumericValueIntVal3"],
                        IntVal3Category = doc.IntVals["LongValueNameForNumericValueIntVal3"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal3"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal3"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal3"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal4 = doc.IntVals["LongValueNameForNumericValueIntVal4"],
                        IntVal4Category = doc.IntVals["LongValueNameForNumericValueIntVal4"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal4"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal4"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal4"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal5 = doc.IntVals["LongValueNameForNumericValueIntVal5"],
                        IntVal5Category = doc.IntVals["LongValueNameForNumericValueIntVal5"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal5"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal5"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal5"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal6 = doc.IntVals["LongValueNameForNumericValueIntVal6"],
                        IntVal6Category = doc.IntVals["LongValueNameForNumericValueIntVal6"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal6"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal6"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal6"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal7 = doc.IntVals["LongValueNameForNumericValueIntVal7"],
                        IntVal7Category = doc.IntVals["LongValueNameForNumericValueIntVal7"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal7"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal7"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal7"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal8 = doc.IntVals["LongValueNameForNumericValueIntVal8"],
                        IntVal8Category = doc.IntVals["LongValueNameForNumericValueIntVal8"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal8"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal8"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal8"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal9 = doc.IntVals["LongValueNameForNumericValueIntVal9"],
                        IntVal9Category = doc.IntVals["LongValueNameForNumericValueIntVal9"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal9"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal9"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal9"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal10 = doc.IntVals["LongValueNameForNumericValueIntVal10"],
                        IntVal10Category = doc.IntVals["LongValueNameForNumericValueIntVal10"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal10"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal10"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal10"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal11 = doc.IntVals["LongValueNameForNumericValueIntVal11"],
                        IntVal11Category = doc.IntVals["LongValueNameForNumericValueIntVal11"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal11"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal11"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal11"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal12 = doc.IntVals["LongValueNameForNumericValueIntVal12"],
                        IntVal12Category = doc.IntVals["LongValueNameForNumericValueIntVal12"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal12"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal12"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal12"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal13 = doc.IntVals["LongValueNameForNumericValueIntVal13"],
                        IntVal13Category = doc.IntVals["LongValueNameForNumericValueIntVal13"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal13"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal13"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal13"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal14 = doc.IntVals["LongValueNameForNumericValueIntVal14"],
                        IntVal14Category = doc.IntVals["LongValueNameForNumericValueIntVal14"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal14"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal14"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 20000000 ? 30 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 30000000 ? 31 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 50000000 ? 32 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 100000000 ? 33 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 200000000 ? 34 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 300000000 ? 35 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal14"].Value < 500000000 ? 36 :
                          37
                        : (int?)null,
                        IntVal15 = doc.IntVals["LongValueNameForNumericValueIntVal15"],
                        IntVal15Category = doc.IntVals["LongValueNameForNumericValueIntVal15"] != null
                        && !double.IsNaN((double)(doc.IntVals["LongValueNameForNumericValueIntVal15"].Value))
                        && !double.IsInfinity((double)(doc.IntVals["LongValueNameForNumericValueIntVal15"].Value))
                        ? (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 0 ? 0 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 1 ? 1 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 2 ? 2 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 3 ? 3 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 5 ? 4 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 10 ? 5 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 20 ? 6 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 30 ? 7 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 50 ? 8 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 100 ? 9 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 200 ? 10 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 300 ? 11 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 500 ? 12 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 1000 ? 13 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 2000 ? 14 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 3000 ? 15 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 5000 ? 16 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 10000 ? 17 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 20000 ? 18 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 30000 ? 19 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 50000 ? 20 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 100000 ? 21 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 200000 ? 22 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 300000 ? 23 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 500000 ? 24 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 1000000 ? 25 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 2000000 ? 26 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 3000000 ? 27 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 5000000 ? 28 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 10000000 ? 29 :
                          (double?)doc.IntVals["LongValueNameForNumericValueIntVal15"].Value < 20000000 ? 30 : // deleting/commenting this line will make the index work.
                          37
                        : (int?)null,
                    };
            }
        }

        public class Doc
        {
            public string Id { get; set; }
            public Dictionary<string, double?> IntVals { get; set; }
        }

        public class DocView
        {
            public string Id { get; set; }
            public double IntVal1 { get; set; }
            public int IntVal1Category { get; set; }
            public double IntVal2 { get; set; }
            public int IntVal2Category { get; set; }
            public double IntVal3 { get; set; }
            public int IntVal3Category { get; set; }
            public double IntVal4 { get; set; }
            public int IntVal4Category { get; set; }
            public double IntVal5 { get; set; }
            public int IntVal5Category { get; set; }
            public double IntVal6 { get; set; }
            public int IntVal6Category { get; set; }
            public double IntVal7 { get; set; }
            public int IntVal7Category { get; set; }
            public double IntVal8 { get; set; }
            public int IntVal8Category { get; set; }
            public double IntVal9 { get; set; }
            public int IntVal9Category { get; set; }
            public double IntVal10 { get; set; }
            public int IntVal10Category { get; set; }
            public double IntVal11 { get; set; }
            public int IntVal11Category { get; set; }
            public double IntVal12 { get; set; }
            public int IntVal12Category { get; set; }
            public double IntVal13 { get; set; }
            public int IntVal13Category { get; set; }
            public double IntVal14 { get; set; }
            public int IntVal14Category { get; set; }
            public double IntVal15 { get; set; }
            public int IntVal15Category { get; set; }
        }
    }
}
