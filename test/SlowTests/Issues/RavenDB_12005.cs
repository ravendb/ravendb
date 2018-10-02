using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12005
    {
        [Fact]
        public void LastIndexOf()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.LastIndexOf('/'), lsv1.LastIndexOf('/'));
                Assert.Equal(fooValue2.LastIndexOf('/'), lsv2.LastIndexOf('/'));
                Assert.Equal(fooValue3.LastIndexOf('/'), lsv3.LastIndexOf('/'));
                Assert.Equal(fooValue4.LastIndexOf('/'), lsv4.LastIndexOf('/'));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue4.LastIndexOf('/'), lsv4.LastIndexOf('/'));
                Assert.Equal(fooValue3.LastIndexOf('/'), lsv3.LastIndexOf('/'));
                Assert.Equal(fooValue2.LastIndexOf('/'), lsv2.LastIndexOf('/'));
                Assert.Equal(fooValue1.LastIndexOf('/'), lsv1.LastIndexOf('/'));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.LastIndexOf(fooValue1), lsv1.LastIndexOf(fooValue1));
                Assert.Equal(fooValue4.LastIndexOf(fooValue1), lsv4.LastIndexOf(fooValue1));


                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.LastIndexOf('/', 4), lsv1.LastIndexOf('/', 4));
                Assert.Equal(fooValue5.LastIndexOf('/', fooValue5.Length - 1, 2), lsv5.LastIndexOf('/', fooValue5.Length - 1, 2));
                Assert.Equal(fooValue5.LastIndexOf('/', fooValue5.Length - 1, 5), lsv5.LastIndexOf('/', fooValue5.Length - 1, 5));
            }
        }

        private static void CreateLazyStringValues(JsonOperationContext context, out string fooValue1, out string fooValue2, out string fooValue3, out string fooValue4, out string fooValue5, out LazyStringValue lsv1, out LazyStringValue lsv2, out LazyStringValue lsv3, out LazyStringValue lsv4, out LazyStringValue lsv5)
        {
            fooValue1 = "Bar/1/2/3/4/5";
            fooValue2 = "Bar/1/2/3/4";
            fooValue3 = "Bar/1/2/3";
            fooValue4 = "Bar/1/2";
            fooValue5 = "Bar/1/2345";

            var djv = new DynamicJsonValue
            {
                ["Foo1"] = fooValue1,
                ["Foo2"] = fooValue2,
                ["Foo3"] = fooValue3,
                ["Foo4"] = fooValue4,
                ["Foo5"] = fooValue5
            };
            var blittable = context.ReadObject(djv, string.Empty);

            lsv1 = blittable["Foo1"] as LazyStringValue;
            lsv2 = blittable["Foo2"] as LazyStringValue;
            lsv3 = blittable["Foo3"] as LazyStringValue;
            lsv4 = blittable["Foo4"] as LazyStringValue;
            lsv5 = blittable["Foo5"] as LazyStringValue;
        }

        [Fact]
        public void LastIndexOfAny()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {

                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                char[] any = new[] { '/', '1' };

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.LastIndexOfAny(any), lsv1.LastIndexOfAny(any));
                Assert.Equal(fooValue2.LastIndexOfAny(any), lsv2.LastIndexOfAny(any));
                Assert.Equal(fooValue3.LastIndexOfAny(any), lsv3.LastIndexOfAny(any));
                Assert.Equal(fooValue4.LastIndexOfAny(any), lsv4.LastIndexOfAny(any));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue4.LastIndexOfAny(any), lsv4.LastIndexOfAny(any));
                Assert.Equal(fooValue3.LastIndexOfAny(any), lsv3.LastIndexOfAny(any));
                Assert.Equal(fooValue2.LastIndexOfAny(any), lsv2.LastIndexOfAny(any));
                Assert.Equal(fooValue1.LastIndexOfAny(any), lsv1.LastIndexOfAny(any));


                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.LastIndexOfAny(any, 4), lsv1.LastIndexOfAny(any, 4));
                Assert.Equal(fooValue5.LastIndexOfAny(any, fooValue5.Length - 1, 2), lsv5.LastIndexOfAny(any, fooValue5.Length - 1, 2));
                Assert.Equal(fooValue5.LastIndexOfAny(any, fooValue5.Length - 1, 5), lsv5.LastIndexOfAny(any, fooValue5.Length - 1, 5));
            }
        }

        [Fact]
        public void IndexOf()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.IndexOf('/'), lsv1.IndexOf('/'));
                Assert.Equal(fooValue2.IndexOf('/'), lsv2.IndexOf('/'));
                Assert.Equal(fooValue3.IndexOf('/'), lsv3.IndexOf('/'));
                Assert.Equal(fooValue4.IndexOf('/'), lsv4.IndexOf('/'));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.IndexOf(fooValue1), lsv1.IndexOf(fooValue1));
                Assert.Equal(fooValue4.IndexOf(fooValue1), lsv4.IndexOf(fooValue1));

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue4.IndexOf('/', fooValue4.Length - 1), lsv4.IndexOf('/', fooValue4.Length - 1));
                Assert.Equal(fooValue3.IndexOf('/', fooValue3.Length - 1), lsv3.IndexOf('/', fooValue3.Length - 1));
                Assert.Equal(fooValue2.IndexOf('/', fooValue2.Length - 1), lsv2.IndexOf('/', fooValue2.Length - 1));
                Assert.Equal(fooValue1.IndexOf('/', fooValue1.Length - 1), lsv1.IndexOf('/', fooValue1.Length - 1));

                Assert.Throws<ArgumentOutOfRangeException>(() => fooValue1.IndexOf('/', fooValue1.Length - 1, 3));
                Assert.Throws<ArgumentOutOfRangeException>(() => lsv1.IndexOf('/', fooValue1.Length - 1, 3));

                Assert.Throws<ArgumentOutOfRangeException>(() => fooValue1.IndexOf("/", fooValue1.Length - 1, 3));
                Assert.Throws<ArgumentOutOfRangeException>(() => lsv1.IndexOf("/", fooValue1.Length - 1, 3));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.IndexOf('/', 5), lsv1.IndexOf('/', 5));
                Assert.Equal(fooValue5.IndexOf('/', 6, 2), lsv5.IndexOf('/', 6, 2));
                Assert.Equal(fooValue5.IndexOf('/', 0, 5), lsv5.IndexOf('/', 0, 5));

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.IndexOf("/"), lsv1.IndexOf("/"));

                Assert.Equal(fooValue1.IndexOf(fooValue4), lsv1.IndexOf(fooValue4));
                Assert.Equal(fooValue1.IndexOf(fooValue4), lsv1.IndexOf(fooValue4));

                Assert.Equal(fooValue4.IndexOf(fooValue1), lsv4.IndexOf(fooValue1));
                Assert.Equal(fooValue4.IndexOf(fooValue1), lsv4.IndexOf(fooValue1));


                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.IndexOf("/", 5), lsv1.IndexOf("/", 5));
                Assert.Equal(fooValue5.IndexOf("/", 6, 2), lsv5.IndexOf("/", 6, 2));
                Assert.Equal(fooValue5.IndexOf("/", 0, 5), lsv5.IndexOf("/", 0, 5));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue4.IndexOf(fooValue1, 5), lsv4.IndexOf(fooValue1, 5));
                Assert.Equal(fooValue1.IndexOf(fooValue4, 5), lsv1.IndexOf(fooValue4, 5));

                Assert.Equal(fooValue4.IndexOf(fooValue1, 0), lsv4.IndexOf(fooValue1, 0));
                Assert.Equal(fooValue1.IndexOf(fooValue4, 0), lsv1.IndexOf(fooValue4, 0));

                Assert.Equal(fooValue4.IndexOf(fooValue1, 5), lsv4.IndexOf(fooValue1, 5));

                Assert.Equal(fooValue1.IndexOf(fooValue4, 0, StringComparison.Ordinal), lsv1.IndexOf(fooValue4, 0, StringComparison.Ordinal));

                Assert.Equal(fooValue1.IndexOf(fooValue4.ToUpper(), 0, StringComparison.Ordinal), lsv1.IndexOf(fooValue4.ToUpper(), 0, StringComparison.Ordinal));

                Assert.Equal(fooValue1.IndexOf(fooValue4, 0, StringComparison.OrdinalIgnoreCase), lsv1.IndexOf(fooValue4, 0, StringComparison.OrdinalIgnoreCase));
                Assert.Equal(fooValue1.IndexOf(fooValue4.ToUpper(), 0, StringComparison.OrdinalIgnoreCase), lsv1.IndexOf(fooValue4.ToUpper(), 0, StringComparison.OrdinalIgnoreCase));



                var anotherString = "FooBarBuzzBum";
                var substring1 = "Buzz";

                var djv = new DynamicJsonValue
                {
                    ["Val"] = anotherString
                };
                var blittable = context.ReadObject(djv, string.Empty);

                var anotherLsv = blittable["Val"] as LazyStringValue;

                Assert.Equal(anotherString.IndexOf(substring1, 0, 2, StringComparison.Ordinal), anotherLsv.IndexOf(substring1, 0, 2, StringComparison.Ordinal));
                Assert.Equal(anotherString.IndexOf(substring1, 0, 6, StringComparison.Ordinal), anotherLsv.IndexOf(substring1, 0, 6, StringComparison.Ordinal));

                Assert.Equal(anotherString.IndexOf(substring1.ToLower(), 0, 2, StringComparison.Ordinal), anotherLsv.IndexOf(substring1.ToLower(), 0, 2, StringComparison.Ordinal));
                Assert.Equal(anotherString.IndexOf(substring1.ToLower(), 0, 6, StringComparison.Ordinal), anotherLsv.IndexOf(substring1.ToLower(), 0, 6, StringComparison.Ordinal));

                Assert.Equal(anotherString.IndexOf(substring1, 0, 2, StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1, 0, 2, StringComparison.OrdinalIgnoreCase));
                Assert.Equal(anotherString.IndexOf(substring1, 0, 6, StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1, 0, 6, StringComparison.OrdinalIgnoreCase));

                Assert.Equal(anotherString.IndexOf(substring1, 0, 2, StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1, 0, 2, StringComparison.OrdinalIgnoreCase));
                Assert.Equal(anotherString.IndexOf(substring1.ToLower(), 0, 6, StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1.ToLower(), 0, 6, StringComparison.OrdinalIgnoreCase));

                Assert.Equal(anotherString.IndexOf(substring1, 0, 2, StringComparison.Ordinal), anotherLsv.IndexOf(substring1, 0, 2, StringComparison.Ordinal));
                Assert.Equal(anotherString.IndexOf(substring1, 0, 6, StringComparison.Ordinal), anotherLsv.IndexOf(substring1, 0, 6, StringComparison.Ordinal));

                Assert.Equal(anotherString.IndexOf(substring1.ToLower(), StringComparison.Ordinal), anotherLsv.IndexOf(substring1.ToLower(), StringComparison.Ordinal));
                Assert.Equal(anotherString.IndexOf(substring1, StringComparison.Ordinal), anotherLsv.IndexOf(substring1, StringComparison.Ordinal));

                Assert.Equal(anotherString.IndexOf(substring1.ToLower(), StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1.ToLower(), StringComparison.OrdinalIgnoreCase));
                Assert.Equal(anotherString.IndexOf(substring1, StringComparison.OrdinalIgnoreCase), anotherLsv.IndexOf(substring1, StringComparison.OrdinalIgnoreCase));
            }
        }

        [Fact]
        public void IndexOfAny()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                char[] any = new[] { '/', '1' };

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.IndexOfAny(any), lsv1.IndexOfAny(any));
                Assert.Equal(fooValue2.IndexOfAny(any), lsv2.IndexOfAny(any));
                Assert.Equal(fooValue3.IndexOfAny(any), lsv3.IndexOfAny(any));
                Assert.Equal(fooValue4.IndexOfAny(any), lsv4.IndexOfAny(any));

                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue4.IndexOfAny(any, fooValue4.Length - 1), lsv4.IndexOfAny(any, fooValue4.Length - 1));
                Assert.Equal(fooValue3.IndexOfAny(any, fooValue3.Length - 1), lsv3.IndexOfAny(any, fooValue3.Length - 1));
                Assert.Equal(fooValue2.IndexOfAny(any, fooValue2.Length - 1), lsv2.IndexOfAny(any, fooValue2.Length - 1));
                Assert.Equal(fooValue1.IndexOfAny(any, fooValue1.Length - 1), lsv1.IndexOfAny(any, fooValue1.Length - 1));

                Assert.Throws<ArgumentOutOfRangeException>(() => fooValue1.IndexOfAny(any, fooValue1.Length - 1, 3));
                Assert.Throws<ArgumentOutOfRangeException>(() => lsv1.IndexOfAny(any, fooValue1.Length - 1, 3));


                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.IndexOfAny(any, 5), lsv1.IndexOfAny(any, 5));
                Assert.Equal(fooValue5.IndexOfAny(any, 6, 2), lsv5.IndexOfAny(any, 6, 2));
                Assert.Equal(fooValue5.IndexOfAny(any, 0, 5), lsv5.IndexOfAny(any, 0, 5));
            }
        }


        [Fact]
        public void EndsWith()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                var str1 = "Foo/1/2/3/a/b";
                var suffix1 = "a/b";
                var suffix2 = "3/a/b";
                var wrongSuffix1 = "3/a";
                var wrongSuffix2 = "a/3";
                var endWithDJV = new DynamicJsonValue()
                {
                    [nameof(str1)] = str1,
                    [nameof(suffix1)] = suffix1,
                    [nameof(suffix2)] = suffix2,
                    [nameof(wrongSuffix1)] = wrongSuffix1,
                    [nameof(wrongSuffix2)] = wrongSuffix2
                };

                var endsWithBlit = context.ReadObject(endWithDJV, string.Empty);

                var str1Lsv = endsWithBlit[nameof(str1)] as LazyStringValue;
                var suffix1Lsv = endsWithBlit[nameof(suffix1)] as LazyStringValue;
                var suffix2Lsv = endsWithBlit[nameof(suffix2)] as LazyStringValue;
                var wrongSuffix1Lsv = endsWithBlit[nameof(wrongSuffix1)] as LazyStringValue;
                var wrongSuffix2Lsv = endsWithBlit[nameof(wrongSuffix2)] as LazyStringValue;

                LazyStringValue.CleanBuffers();

                Assert.Equal(str1.EndsWith(suffix1), str1Lsv.EndsWith(suffix1));
                Assert.Equal(str1.EndsWith(suffix2), str1Lsv.EndsWith(suffix2));
                Assert.Equal(str1.EndsWith(wrongSuffix1), str1Lsv.EndsWith(wrongSuffix1));
                Assert.Equal(str1.EndsWith(wrongSuffix2), str1Lsv.EndsWith(wrongSuffix2));

                LazyStringValue.CleanBuffers();
                Assert.Equal(str1.EndsWith(suffix2), str1Lsv.EndsWith(suffix2));
                Assert.Equal(str1.EndsWith(suffix1), str1Lsv.EndsWith(suffix1));

                LazyStringValue.CleanBuffers();

                Assert.Equal(str1.EndsWith(suffix2.ToUpper()), str1Lsv.EndsWith(suffix2.ToUpper()));
                Assert.Equal(str1.EndsWith(suffix1.ToUpper()), str1Lsv.EndsWith(suffix1.ToUpper()));

                Assert.Equal(str1.EndsWith(suffix2.ToUpper(), true, CultureInfo.CurrentCulture), str1Lsv.EndsWith(suffix2.ToUpper(), true, CultureInfo.CurrentCulture));
                Assert.Equal(str1.EndsWith(suffix2.ToUpper(), false, CultureInfo.CurrentCulture), str1Lsv.EndsWith(suffix2.ToUpper(), false, CultureInfo.CurrentCulture));


                Assert.Equal(str1.EndsWith(suffix1, StringComparison.InvariantCultureIgnoreCase), str1Lsv.EndsWith(suffix1, StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.EndsWith(suffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase), str1Lsv.EndsWith(suffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.EndsWith(wrongSuffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase), str1Lsv.EndsWith(wrongSuffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.EndsWith(wrongSuffix1, StringComparison.InvariantCultureIgnoreCase), str1Lsv.EndsWith(wrongSuffix1, StringComparison.InvariantCultureIgnoreCase));
            }
        }


        [Fact]
        public void Contains()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                var str1 = "Foo/1/2/3/a/b";
                var suffix1 = "a/b";
                var suffix2 = "3/a/b";
                var wrongSuffix1 = "3/a";
                var wrongSuffix2 = "a/3";
                var endWithDJV = new DynamicJsonValue()
                {
                    [nameof(str1)] = str1,
                    [nameof(suffix1)] = suffix1,
                    [nameof(suffix2)] = suffix2,
                    [nameof(wrongSuffix1)] = wrongSuffix1,
                    [nameof(wrongSuffix2)] = wrongSuffix2
                };

                var endsWithBlit = context.ReadObject(endWithDJV, string.Empty);

                var str1Lsv = endsWithBlit[nameof(str1)] as LazyStringValue;
                var suffix1Lsv = endsWithBlit[nameof(suffix1)] as LazyStringValue;
                var suffix2Lsv = endsWithBlit[nameof(suffix2)] as LazyStringValue;
                var wrongSuffix1Lsv = endsWithBlit[nameof(wrongSuffix1)] as LazyStringValue;
                var wrongSuffix2Lsv = endsWithBlit[nameof(wrongSuffix2)] as LazyStringValue;

                LazyStringValue.CleanBuffers();

                Assert.Equal(str1.Contains(suffix1), str1Lsv.Contains(suffix1));
                Assert.Equal(str1.Contains(suffix2), str1Lsv.Contains(suffix2));
                Assert.Equal(str1.Contains(wrongSuffix1), str1Lsv.Contains(wrongSuffix1));
                Assert.Equal(str1.Contains(wrongSuffix2), str1Lsv.Contains(wrongSuffix2));

                LazyStringValue.CleanBuffers();
                Assert.Equal(str1.Contains(suffix2), str1Lsv.Contains(suffix2));
                Assert.Equal(str1.Contains(suffix1), str1Lsv.Contains(suffix1));

                LazyStringValue.CleanBuffers();

                Assert.Equal(str1.Contains("FOO"), str1Lsv.Contains("FOO"));
                Assert.Equal(str1.Contains("foo"), str1Lsv.EndsWith("foo"));

                Assert.Equal(str1.Contains("FOO", StringComparison.OrdinalIgnoreCase), str1Lsv.Contains("FOO", StringComparison.OrdinalIgnoreCase));
                Assert.Equal(str1.Contains("FOO", StringComparison.CurrentCulture), str1Lsv.Contains("FOO", StringComparison.CurrentCulture));


                Assert.Equal(str1.Contains(suffix1, StringComparison.InvariantCultureIgnoreCase), str1Lsv.Contains(suffix1, StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.Contains(suffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase), str1Lsv.Contains(suffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.Contains(wrongSuffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase), str1Lsv.Contains(wrongSuffix1.ToUpper(), StringComparison.InvariantCultureIgnoreCase));
                Assert.Equal(str1.Contains(wrongSuffix1, StringComparison.InvariantCultureIgnoreCase), str1Lsv.Contains(wrongSuffix1, StringComparison.InvariantCultureIgnoreCase));
            }
        }


        [Fact]
        public unsafe void EqualityComparisons()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                LazyStringValue.CleanBuffers();

                var mem = context.GetMemory(lsv1.Size);
                try
                {

                    Memory.Copy(mem.Address, lsv1.Buffer, lsv1.Size);

                    var newLSV1 = new LazyStringValue(null, mem.Address, lsv1.Size, context);



                    Assert.True(lsv1.Equals(fooValue1));
                    Assert.True(lsv1.Equals(lsv1));
                    Assert.True(lsv1.Equals(newLSV1));
                    Assert.True(lsv1.Compare(newLSV1.Buffer, newLSV1.Size) == 0);
                    Assert.True(lsv1.Compare(lsv1.Buffer, lsv1.Size) == 0);

                    Assert.True(lsv1.Compare(lsv2.Buffer, lsv2.Size) > 0);
                    Assert.True(lsv2.Compare(lsv1.Buffer, lsv1.Size) < 0);

                    Assert.True(lsv1.CompareTo(fooValue1) == 0);
                    Assert.True(lsv1.CompareTo(newLSV1) == 0);
                    Assert.True(lsv1.CompareTo(lsv1) == 0);

                    Assert.True(lsv1.CompareTo(lsv2) > 0);
                    Assert.True(lsv2.CompareTo(lsv1) < 0);

                    Assert.True(lsv1.CompareTo(fooValue2) > 0);
                    Assert.True(lsv2.CompareTo(fooValue1) < 0);

                    Assert.True(lsv1 == fooValue1);
                    Assert.True(lsv1 == lsv1);
                    Assert.True(lsv1 == newLSV1);

                    Assert.False(lsv1 == lsv2);
                    Assert.False(lsv1 == fooValue2);

                    Assert.True(lsv1 != lsv2);
                    Assert.True(lsv1 != fooValue2);

                    Assert.False(lsv1 != lsv1);
                    Assert.False(lsv1 != fooValue1);
                }
                finally
                {
                    context.ReturnMemory(mem);
                }



            }
        }

        [Fact]
        public void StartsWith()
        {
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                LazyStringValue.CleanBuffers();

                CreateLazyStringValues(context, out var fooValue1, out var fooValue2, out var fooValue3, out var fooValue4, out var fooValue5, out var lsv1, out var lsv2, out var lsv3, out var lsv4, out var lsv5);

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.StartsWith(fooValue1), lsv1.StartsWith(fooValue1));
                Assert.Equal(fooValue1.StartsWith(fooValue2), lsv1.StartsWith(fooValue2));
                Assert.Equal(fooValue1.StartsWith(fooValue3), lsv1.StartsWith(fooValue3));
                Assert.Equal(fooValue1.StartsWith(fooValue4), lsv1.StartsWith(fooValue4));
                Assert.Equal(fooValue1.StartsWith(fooValue5), lsv1.StartsWith(fooValue5));

                Assert.Equal(fooValue2.StartsWith(fooValue3), lsv2.StartsWith(fooValue3));
                Assert.Equal(fooValue3.StartsWith(fooValue4), lsv3.StartsWith(fooValue4));
                Assert.Equal(fooValue4.StartsWith(fooValue4), lsv4.StartsWith(fooValue4));


                LazyStringValue.CleanBuffers();
                Assert.Equal(fooValue1.StartsWith(fooValue1), lsv1.StartsWith(fooValue1));
                Assert.Equal(fooValue4.StartsWith(fooValue1), lsv4.StartsWith(fooValue1));

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue4.StartsWith(fooValue4), lsv4.StartsWith(fooValue4));
                Assert.Equal(fooValue3.StartsWith(fooValue4), lsv3.StartsWith(fooValue4));
                Assert.Equal(fooValue2.StartsWith(fooValue3), lsv2.StartsWith(fooValue3));
                Assert.Equal(fooValue1.StartsWith(fooValue2), lsv1.StartsWith(fooValue2));

                LazyStringValue.CleanBuffers();

                Assert.Equal(fooValue1.StartsWith("B"), lsv1.StartsWith("B"));
                Assert.Equal(fooValue1.StartsWith("b"), lsv1.StartsWith("b"));
                Assert.Equal(fooValue1.StartsWith("b", true, CultureInfo.CurrentCulture), lsv1.StartsWith("b", true, CultureInfo.CurrentCulture));
                Assert.Equal(fooValue1.StartsWith("b", false, CultureInfo.CurrentCulture), lsv1.StartsWith("b", false, CultureInfo.CurrentCulture));

                Assert.Equal(fooValue1.StartsWith("b", StringComparison.OrdinalIgnoreCase), lsv1.StartsWith("b", StringComparison.OrdinalIgnoreCase));
                Assert.Equal(fooValue1.StartsWith("b", StringComparison.CurrentCulture), lsv1.StartsWith("b", StringComparison.CurrentCulture));

                Assert.Equal(fooValue1.StartsWith('b'), lsv1.StartsWith('b'));
                Assert.Equal(fooValue1.StartsWith('B'), lsv1.StartsWith('B'));
            }
        }
    }
}
