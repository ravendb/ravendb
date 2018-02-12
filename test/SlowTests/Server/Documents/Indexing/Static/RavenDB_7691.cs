using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Server.Documents.Indexing.Static
{
    public class RavenDB_7691 : RavenTestBase
    {
        public class TypeWithDecimal
        {
            public decimal DecimalVal;
        }

        public class TypeWithDouble
        {
            public double DoubleVal;
        }
        public class EdgeCaseValues
        {
            public DateTime DateMaxPercision;
            public DateTimeOffset DateTimeOffsetMaxVal;
            public DateTimeOffset DateTimeOffsetMinVal;            
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
        public async Task ClientAPIDoesNotSupportDecimalBiggerThenDouble()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new TypeWithDecimal()
                    {
                        DecimalVal = decimal.MaxValue
                    });
                    await Assert.ThrowsAsync<NotSupportedException>(() => session.SaveChangesAsync());
                }
                using (var session = store.OpenAsyncSession())
                { 
                    decimal minDoublePercisionDecimal = (decimal)(double.MinValue / Math.Pow(10, 308));
                    decimal maxDoublePercisionDecimal = (decimal)(double.MaxValue / Math.Pow(10, 308));
                    
                    await session.StoreAsync(new TypeWithDecimal()
                    {
                        DecimalVal = minDoublePercisionDecimal
                    }, "docs/1");
                    await session.SaveChangesAsync();

                    await session.StoreAsync(new TypeWithDecimal()
                    {
                        DecimalVal = maxDoublePercisionDecimal
                    }, "docs/2");
                    await session.SaveChangesAsync();

                    var typeWithDecimal = await session.LoadAsync<TypeWithDecimal>("docs/1");
                    Assert.Equal(minDoublePercisionDecimal, typeWithDecimal.DecimalVal);

                    typeWithDecimal = await session.LoadAsync<TypeWithDecimal>("docs/2");
                    Assert.Equal(maxDoublePercisionDecimal, typeWithDecimal.DecimalVal);
                }
            }
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
                FloatMinVal = float.MinValue,
                FloatMaxVal = float.MaxValue,
                FloatMaxPercision = float.Epsilon,
                FloatNegativeInfinity = float.NegativeInfinity,
                FloatPositiveInfinity = float.PositiveInfinity,
                FloatNan = float.NaN,
                UintMaxVal = uint.MaxValue,
                //Currently not supproted
                //UlongMaxVal = ulong.MaxValue,
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

            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200
            }))
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
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMinVal == edgeCaseValues.IntMinVal).ToListAsync());

                    var intMinValPluOne = edgeCaseValues.IntMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMinVal == intMinValPluOne).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == edgeCaseValues.IntMaxVal).ToListAsync());
                    var inmaValMinus1 = edgeCaseValues.IntMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == inmaValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMinVal == edgeCaseValues.LongMinVal).ToListAsync());

                    var longMinValMinus1 = edgeCaseValues.LongMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMinVal == longMinValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x=>x.WaitForNonStaleResults()).Where(x => x.LongMaxVal== edgeCaseValues.LongMaxVal).ToListAsync());

                    var longMaxValMinus1 = edgeCaseValues.LongMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMaxVal == longMaxValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal == edgeCaseValues.DoubleMinVal).ToListAsync());

                    var doubleMinValPlusEpsillon = edgeCaseValues.DoubleMinVal + Math.Pow(10, 292);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal == doubleMinValPlusEpsillon).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal == edgeCaseValues.DoubleMaxVal).ToListAsync());

                    var doubleMaxValMinumEpsillon = edgeCaseValues.DoubleMaxVal - Math.Pow(10, 292);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal == doubleMaxValMinumEpsillon).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity == edgeCaseValues.DoubleNegativeInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity == edgeCaseValues.DoublePositiveInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNan == edgeCaseValues.DoubleNan).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNan == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon == edgeCaseValues.DoubleEpsilon).ToListAsync());

                    var doubleEpsillonTimes2 = edgeCaseValues.DoubleEpsilon * 2;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon == doubleEpsillonTimes2).ToListAsync());
                    
                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMinVal == edgeCaseValues.FloatMinVal).ToListAsync());

                    float floatMinValPlus1 = edgeCaseValues.FloatMinVal + (float)Math.Pow(10, 32);
                    var v = session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMinVal == floatMinValPlus1);
                    Assert.Empty(await v.ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMaxVal == edgeCaseValues.FloatMaxVal).ToListAsync());

                    float floatMaxValuMinus1 = edgeCaseValues.FloatMaxVal - (float)Math.Pow(10, 32);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMaxVal == floatMaxValuMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMaxPercision == edgeCaseValues.FloatMaxPercision).ToListAsync());

                    float floatMaxPercisionTimes2 = edgeCaseValues.FloatMaxPercision * 2;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatMaxPercision == floatMaxPercisionTimes2).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatNegativeInfinity == edgeCaseValues.FloatNegativeInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatNegativeInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatPositiveInfinity == edgeCaseValues.FloatPositiveInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatPositiveInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatNan == edgeCaseValues.FloatNan).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.FloatNan == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UintMaxVal == edgeCaseValues.UintMaxVal).ToListAsync());

                    uint uintMaxValuMinus1 = edgeCaseValues.UintMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UintMaxVal == uintMaxValuMinus1).ToListAsync());

                    //Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal == edgeCaseValues.UlongMaxVal).ToListAsync());

                    //ulong ulongMaxValMinus1 = edgeCaseValues.UlongMaxVal - 1;
                    //Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal == ulongMaxValMinus1).ToListAsync());

                    //Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringMaxLength == edgeCaseValues.StringMaxLength).ToListAsync());

                    //int stringMaxLengthMinus1 = edgeCaseValues.StringMaxLength.Length - 1;
                    //Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.StringMaxLength == edgeCaseValues.StringMaxLength.Substring(0, stringMaxLengthMinus1)).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateMaxPercision == edgeCaseValues.DateMaxPercision).ToListAsync());

                    DateTime datePercisionPlusMinute = edgeCaseValues.DateMaxPercision.AddMinutes(1);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateMaxPercision == datePercisionPlusMinute).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMinVal == edgeCaseValues.DateTimeOffsetMinVal).ToListAsync());

                    DateTimeOffset dateTimeOffsetPlusMillisecond = edgeCaseValues.DateTimeOffsetMinVal.AddMilliseconds(1);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMinVal == dateTimeOffsetPlusMillisecond).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMaxVal == edgeCaseValues.DateTimeOffsetMaxVal).ToListAsync());

                    DateTimeOffset dateTimeOffsetMinusMillisecond = edgeCaseValues.DateTimeOffsetMaxVal.AddMilliseconds(-1);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DateTimeOffsetMaxVal == dateTimeOffsetMinusMillisecond).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinVal == edgeCaseValues.TimeSpanMinVal).ToListAsync());

                    TimeSpan timespanMinValPlusTick = edgeCaseValues.TimeSpanMinVal.Add(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinVal == timespanMinValPlusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMaxVal == edgeCaseValues.TimeSpanMaxVal).ToListAsync());

                    TimeSpan timespanMaxValMinusTick = edgeCaseValues.TimeSpanMaxVal.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMaxVal == timespanMaxValMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanDays == edgeCaseValues.TimeSpanDays).ToListAsync());

                    TimeSpan timepsanDayMinusTick = edgeCaseValues.TimeSpanDays.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanDays == timepsanDayMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanHours == edgeCaseValues.TimeSpanHours).ToListAsync());

                    TimeSpan timespanHourMinusTick = edgeCaseValues.TimeSpanHours.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanHours == timespanHourMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinutes == edgeCaseValues.TimeSpanMinutes).ToListAsync());

                    TimeSpan timeSpanMinuteMinusTick = edgeCaseValues.TimeSpanMinutes.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMinutes == timeSpanMinuteMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanSeconds == edgeCaseValues.TimeSpanSeconds).ToListAsync());

                    TimeSpan timespanSecondMinusTick = edgeCaseValues.TimeSpanSeconds.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanSeconds == timespanSecondMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMiliseconds == edgeCaseValues.TimeSpanMiliseconds).ToListAsync());

                    TimeSpan timespanMillisecondMinusTick = edgeCaseValues.TimeSpanMiliseconds.Subtract(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanMiliseconds == timespanMillisecondMinusTick).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanNanoseconds == edgeCaseValues.TimeSpanNanoseconds).ToListAsync());

                    TimeSpan timeSpanNanosecondPlusTick = edgeCaseValues.TimeSpanNanoseconds.Add(TimeSpan.FromTicks(1));
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.TimeSpanNanoseconds == timeSpanNanosecondPlusTick).ToListAsync());


                }
            }
        }
    }
}
