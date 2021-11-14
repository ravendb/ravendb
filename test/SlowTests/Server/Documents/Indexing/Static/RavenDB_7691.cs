using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.JavaScript;
using Sparrow.Extensions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.Static
{
    public class RavenDB_7691 : RavenTestBase
    {
        public RavenDB_7691(ITestOutputHelper output) : base(output)
        {
        }

    private string QueryWithScalarToRawStringForAllValues(string jsEngineType)
        {
            var prefix = jsEngineType == "Jint" ? "u=> u." : "\"";
            var postfix = jsEngineType == "Jint" ? "" : "\"";
            return @$"declare function MyProjection(x){{
return {{
    IntMinVal : scalarToRawString(x, {prefix}IntMinVal{postfix}),
    IntMaxVal : scalarToRawString(x, {prefix}IntMaxVal{postfix}),
    LongMinVal : scalarToRawString(x, {prefix}LongMinVal{postfix}),
    LongMaxVal : scalarToRawString(x, {prefix}LongMaxVal{postfix}),
    DecimalMaxVal : scalarToRawString(x, {prefix}DecimalMaxVal{postfix}),
    DecimalMinVal : scalarToRawString(x, {prefix}DecimalMinVal{postfix}),
    DoubleMinVal : scalarToRawString(x, {prefix}DoubleMinVal{postfix}),
    DoubleMaxVal : scalarToRawString(x, {prefix}DoubleMaxVal{postfix}),
    DoubleNegativeInfinity : scalarToRawString(x, {prefix}DoubleNegativeInfinity{postfix}),
    DoublePositiveInfinity : scalarToRawString(x, {prefix}DoublePositiveInfinity{postfix}),
    DoubleNan : scalarToRawString(x, {prefix}DoubleNan{postfix}),
    DoubleEpsilon : scalarToRawString(x, {prefix}DoubleEpsilon{postfix}),
    FloatMinVal : scalarToRawString(x, {prefix}FloatMinVal {postfix}),
    FloatMaxVal : scalarToRawString(x, {prefix}FloatMaxVal{postfix}),
    FloatMaxPercision : scalarToRawString(x, {prefix}FloatMaxPercision{postfix}),
    FloatNegativeInfinity : scalarToRawString(x, {prefix}FloatNegativeInfinity{postfix}),
    FloatPositiveInfinity : scalarToRawString(x, {prefix}FloatPositiveInfinity{postfix}),
    FloatNan : scalarToRawString(x, {prefix}FloatNan{postfix}),
    UintMaxVal : scalarToRawString(x, {prefix}UintMaxVal{postfix}),
    UlongMaxVal : scalarToRawString(x, {prefix}UlongMaxVal{postfix}),
    StringMaxLength : scalarToRawString(x, {prefix}StringMaxLength{postfix}),
    DateMaxPercision : scalarToRawString(x, {prefix}DateMaxPercision{postfix}),
    DateTimeOffsetMinVal : scalarToRawString(x, {prefix}DateTimeOffsetMinVal{postfix}),
    DateTimeOffsetMaxVal : scalarToRawString(x, {prefix}DateTimeOffsetMaxVal{postfix}),
    TimeSpanMinVal : scalarToRawString(x, {prefix}TimeSpanMinVal{postfix}),
    TimeSpanMaxVal : scalarToRawString(x, {prefix}TimeSpanMaxVal{postfix}),
    TimeSpanDays : scalarToRawString(x, {prefix}TimeSpanDays{postfix}),
    TimeSpanHours : scalarToRawString(x, {prefix}TimeSpanHours{postfix}),
    TimeSpanMinutes : scalarToRawString(x, {prefix}TimeSpanMinutes{postfix}),
    TimeSpanSeconds : scalarToRawString(x, {prefix}TimeSpanSeconds{postfix}),
    TimeSpanMiliseconds : scalarToRawString(x, {prefix}TimeSpanMiliseconds{postfix}),
    TimeSpanNanoseconds : scalarToRawString(x, {prefix}TimeSpanNanoseconds{postfix})
}}
}}
from EdgeCaseValues as e select MyProjection(e)";
        }

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
            public decimal DecimalMaxVal;
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
            public double simpleDouble;
        }

        public class EdgeCaseValuesX
        {
            public DateTime DateMaxPercisionX;
            public DateTimeOffset DateTimeOffsetMaxValX;
            public DateTimeOffset DateTimeOffsetMinValX;
            public decimal DecimalMaxValX;
            public decimal DecimalMinValX;
            public double DoubleEpsilonX;
            public double DoubleMaxValX;
            public double DoubleMinValX;
            public double DoubleNanX;
            public double DoubleNegativeInfinityX;
            public double DoublePositiveInfinityX;
            public float FloatMaxPercisionX;
            public float FloatMaxValX;
            public float FloatMinValX;
            public float FloatNanX;
            public float FloatNegativeInfinityX;
            public float FloatPositiveInfinityX;
            public int IntMaxValX;
            public int IntMinValX;
            public long LongMaxValX;
            public long LongMinValX;
            public string StringMaxLengthX;
            public TimeSpan TimeSpanDaysX;
            public TimeSpan TimeSpanHoursX;
            public TimeSpan TimeSpanMaxValX;
            public TimeSpan TimeSpanMilisecondsX;
            public TimeSpan TimeSpanMinutesX;
            public TimeSpan TimeSpanMinValX;
            public TimeSpan TimeSpanNanosecondsX;
            public TimeSpan TimeSpanSecondsX;
            public uint UintMaxValX;
            public ulong UlongMaxValX;
            public double simpleDoubleX;
        }

        [Fact]
        public async Task CanParseFieldsEdgeCasesValuesInDocuments()
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCaseValues();
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
                    AssertFieldsValuesEqual(edgeCaseValues, edgeCaseDeserialized);
                }
            }
        }

        // TODO [shlomo] RavenDB-18121: V8 version should be activated
        [Theory]
        [JavaScriptEngineClassData(JavaScriptEngineType.Jint)]
        public async Task CanParseNumericEdgeCasesRawValuesInJSProjection(string jsEngineType)
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCaseValues();

            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200,
                ModifyDatabaseRecord = Options.ModifyForJavaScriptEngine(jsEngineType)
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        QueryWithScalarToRawStringForAllValues(jsEngineType)
                    ).FirstAsync();

                    AssertFieldsValuesEqual(edgeCaseValues, edgeCaseDeserialized);
                }
            }
        }

        // TODO [shlomo] RavenDB-18121: V8 version should be activated
        [Theory]
        [JavaScriptEngineClassData(JavaScriptEngineType.Jint)]
        public async Task CanParseNumericPercisionEdgeCasesRawValuesInJSProjection(string jsEngineType)
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCasePercisionValues();

            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200,
                ModifyDatabaseRecord = Options.ModifyForJavaScriptEngine(jsEngineType)
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        QueryWithScalarToRawStringForAllValues(jsEngineType)
                    ).FirstAsync();

                    AssertFieldsValuesEqual(edgeCaseValues, edgeCaseDeserialized);
                }
            }
        }

        // TODO [shlomo] RavenDB-18121: V8 version should be activated
        [Theory]
        [JavaScriptEngineClassData(JavaScriptEngineType.Jint)]
        public async Task ScalarToRawThrowsOnIllegalLambdas(string jsEngineType)
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCaseValues();
            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200,
                ModifyDatabaseRecord = Options.ModifyForJavaScriptEngine(jsEngineType)
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();
                }

                // TODO [shlomo] this should work in the commented version as well
                var prefix = "u=> u."; //jsEngineType == "Jint" ? "u=> u." : "\"";
                var postfix = ""; //"jsEngineType == "Jint" ? "" : "\"";

                // modify, then access raw, then access regular
                using (var session = store.OpenAsyncSession())
                {
                    await Assert.ThrowsAsync<RavenException>(() => session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
x.IntMinVal = 4;
var intMinValRaw = parseInt(scalarToRawString(x, {prefix}IntMinVal.OtherVal{postfix}).toString());
x.IntMinVal = intMinValRaw;
var intMinValOriginal = x.IntMinVal;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync());

                    /*await Assert.ThrowsAsync<RavenException>(() => session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                      @"declare function MyProjection(x){
x.IntMinVal = 4;
var intMinValRaw = parseInt(scalarToRawString(x, u=> 4).toString());
x.IntMinVal = intMinValRaw;
var intMinValOriginal = x.IntMinVal;
return x;
}
from EdgeCaseValues as e select MyProjection(e)"
                  ).FirstAsync());

                    await Assert.ThrowsAsync<RavenException>(() => session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                    @"declare function MyProjection(x){
x.IntMinVal = 4;
var intMinValRaw = parseInt(scalarToRawString(x, 4).toString());
x.IntMinVal = intMinValRaw;
var intMinValOriginal = x.IntMinVal;
return x;
}
from EdgeCaseValues as e select MyProjection(e)"
                ).FirstAsync());

                    await Assert.ThrowsAsync<RavenException>(() => session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                    @$"declare function MyProjection(x){{
x.IntMinVal = 4;
var intMinValRaw = parseInt(scalarToRawString(x, {prefix}IntMinVal + 5{postfix}).toString());
x.IntMinVal = intMinValRaw;
var intMinValOriginal = x.IntMinVal;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                ).FirstAsync());*/
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public async Task CanModifyRawAndOriginalValuesTogether(string jsEngineType)
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCaseValues();

            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200,
                ModifyDatabaseRecord = Options.ModifyForJavaScriptEngine(jsEngineType)
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();
                }

                // TODO [shlomo] this should work in the commented version as well
                var prefix = "u=> u."; //jsEngineType == "Jint" ? "u=> u." : "\"";
                var postfix = ""; //"jsEngineType == "Jint" ? "" : "\"";

                // modify, then access raw, then access regular
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
x.IntMinVal = 4;
var intMinValRaw = parseInt(scalarToRawString(x, {prefix}IntMinVal{postfix}).toString());
x.IntMinVal = intMinValRaw;
var intMinValOriginal = x.IntMinVal;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    Assert.Equal(4, edgeCaseDeserialized.IntMinVal);
                }

                // modify, then access regular, then access raw
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
x.IntMinVal = 4;
var intMinValOriginal = x.IntMinVal;
x.IntMinVal = intMinValOriginal;
var intMinValRaw = parseInt(scalarToRawString(x, {prefix}IntMinVal{postfix}).toString());

return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    // TODO: we should probably document this behavior
                    Assert.Equal(4, edgeCaseDeserialized.IntMinVal);
                }

                // access raw, then original, then modify
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
var intMinValRaw = scalarToRawString(x, {prefix}IntMinVal{postfix});
var intMinValOriginal = x.IntMinVal;
x.IntMinVal = 4;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    Assert.Equal(4, edgeCaseDeserialized.IntMinVal);
                }

                // access original, then raw, then modify
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
var intMinValOriginal = x.IntMinVal;
var intMinValRaw = scalarToRawString(x, {prefix}IntMinVal{postfix});
x.IntMinVal = 4;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    Assert.Equal(4, edgeCaseDeserialized.IntMinVal);
                }

                // same, with decimal
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
var intMinValOriginal = x.DecimalMaxVal;
var intMinValRaw = scalarToRawString(x, {prefix}DecimalMaxVal{postfix});
x.DecimalMaxVal = 4;
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    Assert.Equal(4, edgeCaseDeserialized.DecimalMaxVal);
                }

                // same, with string
                using (var session = store.OpenAsyncSession())
                {
                    var edgeCaseDeserialized = await session.Advanced.AsyncRawQuery<EdgeCaseValues>(
                        @$"declare function MyProjection(x){{
var intMinValOriginal = x.StringMaxLength;
var intMinValRaw = scalarToRawString(x, {prefix}StringMaxLength{postfix});
x.StringMaxLength = 'shorter string';
return x;
}}
from EdgeCaseValues as e select MyProjection(e)"
                    ).FirstAsync();

                    Assert.Equal("shorter string", edgeCaseDeserialized.StringMaxLength);
                }
            }
        }

        private static void AssertFieldsValuesEqual(EdgeCaseValues edgeCaseValues, EdgeCaseValues edgeCaseDeserialized)
        {
            var edgeCaseValuesType = typeof(EdgeCaseValues);
            foreach (var field in edgeCaseValuesType.GetFields(BindingFlags.Instance | BindingFlags.Public))
            {
                var value1 = field.GetValue(edgeCaseValues);
                var value2 = field.GetValue(edgeCaseDeserialized);

                if (value1 is double dbl1 && value2 is double dbl2)
                    Assert.True(dbl1.AlmostEquals(dbl2));
                else if (value1 is float flt1 && value2 is float flt2)
                    Assert.True(flt1.AlmostEquals(flt2));
                else
                    Assert.Equal(value1, value2);
            }
        }

        [Fact(Skip = "RavenDB-15952")]
        public async Task CanIndexBigNumbersEdgeCases()
        {
            EdgeCaseValues edgeCaseValues = GenerateEdgeCaseValues();

            using (var store = GetDocumentStore(new Options()
            {
                ModifyDocumentStore = x => x.Conventions.MaxNumberOfRequestsPerSession = 200
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(edgeCaseValues);
                    await session.SaveChangesAsync();

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMinVal == edgeCaseValues.IntMinVal).ToListAsync());

                    var intMinValPluOne = edgeCaseValues.IntMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMinVal == intMinValPluOne).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == edgeCaseValues.IntMaxVal).ToListAsync());
                    var inmaValMinus1 = edgeCaseValues.IntMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.IntMaxVal == inmaValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMinVal == edgeCaseValues.LongMinVal).ToListAsync());

                    var longMinValMinus1 = edgeCaseValues.LongMinVal + 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMinVal == longMinValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMaxVal == edgeCaseValues.LongMaxVal).ToListAsync());

                    var longMaxValMinus1 = edgeCaseValues.LongMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMaxVal == longMaxValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMaxVal == edgeCaseValues.LongMaxVal).ToListAsync());

                    var ulongMaxValMinus1 = edgeCaseValues.UlongMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal == ulongMaxValMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.LongMaxVal == edgeCaseValues.LongMaxVal).ToListAsync());

                    var doubleMinValPlusEpsillon = edgeCaseValues.DoubleMinVal + Math.Pow(10, 292);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal == doubleMinValPlusEpsillon).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal == edgeCaseValues.DoubleMaxVal).ToListAsync());

                    var doubleMaxValMinumEpsillon = edgeCaseValues.DoubleMaxVal - Math.Pow(10, 292);
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMaxVal == doubleMaxValMinumEpsillon).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleMinVal == edgeCaseValues.DoubleMinVal).ToListAsync());

                    var decimalMinValPlusOne = (edgeCaseValues.DecimalMinVal + 1).ToString("G");
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DecimalMinVal.ToString() == decimalMinValPlusOne).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DecimalMinVal.ToString() == edgeCaseValues.DecimalMinVal.ToString()).ToListAsync());

                    // todo: RavenDB-10603
                    //var decimalMaxValMinusOne = edgeCaseValues.DecimalMaxVal - 1;
                    //Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DecimalMaxVal == decimalMaxValMinusOne).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity == edgeCaseValues.DoubleNegativeInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNegativeInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity == edgeCaseValues.DoublePositiveInfinity).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoublePositiveInfinity == 0).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNan == edgeCaseValues.DoubleNan).ToListAsync());
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleNan == 0).ToListAsync());

                    // TODO: RavenDB-16692
                    // Since RavenDB 5.0 we are using numeric _L_ and _D_ fields for number comparison. In this case the _D_ field will be used and Lucene does not handle floating point comparison well
                    //Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon == edgeCaseValues.DoubleEpsilon).ToListAsync());
                    Assert.NotEmpty(await session.Advanced.AsyncDocumentQuery<EdgeCaseValues>().WaitForNonStaleResults().WhereEquals("DoubleEpsilon", edgeCaseValues.DoubleEpsilon.ToString("G"), exact: true).ToListAsync());

                    // TODO: RavenDB-16692
                    // Since RavenDB 5.0 we are using numeric _L_ and _D_ fields for number comparison. In this case the _D_ field will be used and Lucene does not handle floating point comparison well
                    //var doubleEpsillonTimes3 = edgeCaseValues.DoubleEpsilon * 3;
                    //Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.DoubleEpsilon == doubleEpsillonTimes3).ToListAsync());
                    var doubleEpsillonTimes3 = (edgeCaseValues.DoubleEpsilon * 3).ToString("G");
                    Assert.Empty(await session.Advanced.AsyncDocumentQuery<EdgeCaseValues>().WaitForNonStaleResults().WhereEquals("DoubleEpsilon", doubleEpsillonTimes3, exact: true).ToListAsync());

                    /* todo: RavenDB-15952
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
                    */

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UintMaxVal == edgeCaseValues.UintMaxVal).ToListAsync());

                    uint uintMaxValuMinus1 = edgeCaseValues.UintMaxVal - 1;
                    Assert.Empty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UintMaxVal == uintMaxValuMinus1).ToListAsync());

                    Assert.NotEmpty(await session.Query<EdgeCaseValues>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.UlongMaxVal == edgeCaseValues.UlongMaxVal).ToListAsync());

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

        private static EdgeCaseValues GenerateEdgeCasePercisionValues()
        {
            return new EdgeCaseValues
            {
                DecimalMaxVal = long.MaxValue,
                DecimalMinVal = long.MinValue,
                DoubleMinVal = long.MaxValue,
                DoubleMaxVal = long.MinValue,
                FloatMinVal = int.MinValue,
                FloatMaxVal = int.MaxValue
            };
        }

        private static EdgeCaseValues GenerateEdgeCaseValues()
        {
            return new EdgeCaseValues
            {
                IntMinVal = int.MinValue,
                IntMaxVal = int.MaxValue,
                LongMinVal = long.MinValue,
                LongMaxVal = long.MaxValue,
                DecimalMaxVal = decimal.MaxValue,
                DecimalMinVal = decimal.MinValue,
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
        }
    }
}
