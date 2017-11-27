using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Xunit;

namespace FastTests.Server.Documents.Indexing.Static
{
    public class RavenDB_7691 : RavenTestBase
    {
        public class EdgeCaseValues
        {
            public DateTime DateMaxPercision;
            public DateTimeOffset DateTimeOffsetMaxVal;
            public DateTimeOffset DateTimeOffsetMinVal;
            public decimal DecimalMaxVal;
            public decimal DecimalIntMaxPercision;
            public decimal DecimalFloatMaxPercision { get; set; }
            public decimal DecimalMinVal;
            public double DoubleEpsilon;
            public double DoubleMaxVal;
            public double DoubleMinVal;
            public double DoubleNan;
            public double DoubleNegativeInfinity;
            public double DoublePositiveInfinity;
            public float FloatMaxPercision;
            public float FloatMaxVal;
            public float FloatMinVal;
            public float FloatNan;
            public float FloatNegativeInfinity;
            public float FloatPositiveInfinity;
            public int IntMaxVal;
            public int IntMinVal;
            public long LongMaxVal;
            public long LongMinVal;
            public string StringMaxLength;
            public TimeSpan TimeSpanDays;
            public TimeSpan TimeSpanHours;
            public TimeSpan TimeSpanMaxVal;
            public TimeSpan TimeSpanMiliseconds;
            public TimeSpan TimeSpanMinutes;
            public TimeSpan TimeSpanMinVal;
            public TimeSpan TimeSpanNanoseconds;
            public TimeSpan TimeSpanSeconds;
            public uint UintMaxVal;
            public ulong UlongMaxVal;
            
        }

        [Fact]
        public async Task CanIndexBigNumbersEdgeCases()
        {
            var edgeCaseValues = new EdgeCaseValues
            {
                IntMinVal = int.MinValue,
                IntMaxVal = int.MaxValue,
                LongMinVal = long.MinValue,
                LongMaxVal = long.MaxValue,
                DoubleMinVal = double.MinValue,
                DoubleMaxVal = double.MaxValue,
                DoubleNegativeInfinity = double.NegativeInfinity,
                DoublePositiveInfinity = double.PositiveInfinity,
                DoubleNan = double.NaN,
                DoubleEpsilon = double.Epsilon,
                DecimalMinVal = decimal.MinValue,
                DecimalMaxVal = decimal.MaxValue,
                DecimalIntMaxPercision = decimal.Parse(string.Join("", Enumerable.Repeat(1, 28))),
                DecimalFloatMaxPercision = decimal.Parse("0."+string.Join("", Enumerable.Repeat(1, 28))),
                FloatMinVal = float.MinValue,
                FloatMaxVal = float.MaxValue,
                FloatMaxPercision = float.Epsilon,
                FloatNegativeInfinity = float.NegativeInfinity,
                FloatPositiveInfinity = float.PositiveInfinity,
                FloatNan = float.NaN,
                UintMaxVal = uint.MaxValue,
                UlongMaxVal = ulong.MaxValue,
                StringMaxLength = string.Join("", Enumerable.Repeat(1, short.MaxValue)),
                DateMaxPercision = DateTime.Now,
                DateTimeOffsetMinVal = DateTimeOffset.MinValue,
                DateTimeOffsetMaxVal = DateTimeOffset.MaxValue,
                TimeSpanMinVal = TimeSpan.MinValue,
                TimeSpanMaxVal = TimeSpan.MaxValue,
                TimeSpanDays = TimeSpan.FromDays(1),
                TimeSpanHours = TimeSpan.FromHours(1),
                TimeSpanMinutes = TimeSpan.FromMinutes(1),
                TimeSpanSeconds = TimeSpan.FromSeconds(1),
                TimeSpanMiliseconds = TimeSpan.FromMilliseconds(1),
                TimeSpanNanoseconds = TimeSpan.FromTicks(1)
            };

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Query<EdgeCaseValues>().FirstAsync();

                    var edgeCaseValuesType = typeof(EdgeCaseValues);
                    foreach (var field in edgeCaseValuesType.GetFields(BindingFlags.Instance | BindingFlags.Public))
                    {
                        Assert.Equal(field.GetValue(edgeCaseValues), field.GetValue(edgeCaseDeserialized));
                    }
                    Assert.Equal(edgeCaseValues.DecimalMinVal, edgeCaseDeserialized.DecimalMinVal);
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.IntMinVal == edgeCaseValues.IntMinVal).ToListAsync());
                    
                    var intMinValPluOne = edgeCaseValues.IntMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.IntMinVal == intMinValPluOne).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == edgeCaseValues.IntMaxVal).ToListAsync());
                    var inmaValMinus1 = edgeCaseValues.IntMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == inmaValMinus1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.LongMinVal== edgeCaseValues.LongMinVal).ToListAsync());

                    var longMinValMinus1 = edgeCaseValues.LongMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.LongMinVal== longMinValMinus1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.LongMaxVal== edgeCaseValues.LongMaxVal).ToListAsync());
                    
                    var longMaxValMinus1 = edgeCaseValues.LongMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.LongMaxVal== longMaxValMinus1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal== edgeCaseValues.DoubleMinVal).ToListAsync());

                    var doubleMinValPlusEpsillon = edgeCaseValues.DoubleMinVal + Double.Epsilon;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal== doubleMinValPlusEpsillon).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal== edgeCaseValues.DoubleMaxVal).ToListAsync());

                    var doubleMaxValMinumEpsillon = edgeCaseValues.DoubleMaxVal - Double.Epsilon; 
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal== doubleMaxValMinumEpsillon).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity== edgeCaseValues.DoubleNegativeInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity== edgeCaseValues.DoublePositiveInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleNan== edgeCaseValues.DoubleNan).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleNan== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon== edgeCaseValues.DoubleEpsilon).ToListAsync());

                    var doubleEpsillonTimes2 = edgeCaseValues.DoubleEpsilon * 2;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon== doubleEpsillonTimes2).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalMinVal== edgeCaseValues.DecimalMinVal).ToListAsync());

                    var decimalMinValPlus1 = edgeCaseValues.DecimalMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalMinVal== decimalMinValPlus1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalMaxVal== edgeCaseValues.DecimalMaxVal).ToListAsync());

                    var decimalMaxValMinus1 = edgeCaseValues.DecimalMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalMaxVal== decimalMaxValMinus1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalIntMaxPercision== edgeCaseValues.DecimalIntMaxPercision).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalIntMaxPercision== edgeCaseValues.DecimalIntMaxPercision).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalFloatMaxPercision== edgeCaseValues.DecimalFloatMaxPercision).ToListAsync());

                    var decimalFloatMaxPercisionPlus = edgeCaseValues.DecimalFloatMaxPercision + decimal.Parse("0." + string.Join("", Enumerable.Repeat(0, 27)) + 1);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DecimalFloatMaxPercision== decimalFloatMaxPercisionPlus).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMinVal== edgeCaseValues.FloatMinVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMinVal== edgeCaseValues.FloatMinVal+1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMaxVal== edgeCaseValues.FloatMaxVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMaxVal== edgeCaseValues.FloatMaxVal-1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMaxPercision== edgeCaseValues.FloatMaxPercision).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatMaxPercision== edgeCaseValues.FloatMaxPercision *2).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatNegativeInfinity== edgeCaseValues.FloatNegativeInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatNegativeInfinity== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatPositiveInfinity== edgeCaseValues.FloatPositiveInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatPositiveInfinity== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatNan== edgeCaseValues.FloatNan).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.FloatNan== 0).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.UintMaxVal== edgeCaseValues.UintMaxVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.UintMaxVal== edgeCaseValues.UintMaxVal-1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal== edgeCaseValues.UlongMaxVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal== edgeCaseValues.UlongMaxVal-1).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.StringMaxLength== edgeCaseValues.StringMaxLength).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.StringMaxLength== edgeCaseValues.StringMaxLength.Substring(0,edgeCaseValues.StringMaxLength.Length-1)).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateMaxPercision == edgeCaseValues.DateMaxPercision).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateMaxPercision== edgeCaseValues.DateMaxPercision.AddMinutes(1)).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMinVal== edgeCaseValues.DateTimeOffsetMinVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMinVal== edgeCaseValues.DateTimeOffsetMinVal.AddMilliseconds(1)).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMaxVal== edgeCaseValues.DateTimeOffsetMaxVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMaxVal== edgeCaseValues.DateTimeOffsetMaxVal.AddMilliseconds(-1)).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinVal== edgeCaseValues.TimeSpanMinVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinVal== edgeCaseValues.TimeSpanMinVal.Add(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMaxVal== edgeCaseValues.TimeSpanMaxVal).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMaxVal== edgeCaseValues.TimeSpanMinVal.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanDays== edgeCaseValues.TimeSpanDays).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanDays== edgeCaseValues.TimeSpanDays.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanHours== edgeCaseValues.TimeSpanHours).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanHours== edgeCaseValues.TimeSpanHours.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinutes== edgeCaseValues.TimeSpanMinutes).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinutes== edgeCaseValues.TimeSpanMinutes.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanSeconds== edgeCaseValues.TimeSpanSeconds).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanSeconds== edgeCaseValues.TimeSpanSeconds.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMiliseconds== edgeCaseValues.TimeSpanMiliseconds).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanMiliseconds== edgeCaseValues.TimeSpanMiliseconds.Subtract(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanNanoseconds== edgeCaseValues.TimeSpanNanoseconds).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.TimeSpanNanoseconds== edgeCaseValues.TimeSpanNanoseconds.Add(TimeSpan.FromTicks(1))).ToListAsync());
                    
                    
                }
            }
        }
    }
}
