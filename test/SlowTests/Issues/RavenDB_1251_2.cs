using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_1251_2 : RavenTestBase
    {
        public class Foo
        {
            public Duration Bar { get; set; }
        }

        [Fact]
        public void Duration_Can_Sort_By_Range_Value()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = str =>
                {
                    str.Conventions.CustomizeJsonSerializer = s => s.Converters.Add(new DurationConverter());
                    str.Conventions.RegisterQueryValueConverter<Duration>(DurationQueryValueConverter, RangeType.Long);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(-2)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(-1)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(0)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(1)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(2)) });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.Bar);
                    Debug.WriteLine(q);
                    var result = q.ToList();

                    Assert.Equal(5, result.Count);
                    Assert.True(result[0].Bar > result[1].Bar);
                    Assert.True(result[1].Bar > result[2].Bar);
                    Assert.True(result[2].Bar > result[3].Bar);
                    Assert.True(result[3].Bar > result[4].Bar);
                }
            }
        }

        [Fact]
        public void Duration_Can_Filter_By_Value()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = str =>
                {
                    str.Conventions.CustomizeJsonSerializer = s => s.Converters.Add(new DurationConverter());
                    str.Conventions.RegisterQueryValueConverter<Duration>(DurationQueryValueConverter, RangeType.Long);
                }
            }))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(-2)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(-1)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(0)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(1)) });
                    session.Store(new Foo { Bar = new Duration(TimeSpan.FromHours(2)) });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var d = new Duration(TimeSpan.FromHours(-1.5));

                    var q = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Bar > d)
                        .OrderByDescending(x => x.Bar);
                    Debug.WriteLine(q);
                    var result = q.ToList();

                    Assert.Equal(4, result.Count);
                    Assert.True(result[0].Bar > result[1].Bar);
                    Assert.True(result[1].Bar > result[2].Bar);
                    Assert.True(result[2].Bar > result[3].Bar);
                    Assert.Equal(new Duration(TimeSpan.FromHours(-1)), result[3].Bar);
                }
            }
        }


        public struct Duration : IEquatable<Duration>, IComparable<Duration>, IComparable
        {
            public long Ticks { get; private set; }

            public Duration(TimeSpan ts)
                : this()
            {
                Ticks = ts.Ticks;
            }

            public override string ToString()
            {
                return new TimeSpan(Ticks).ToString("c");
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                return obj is Duration && Equals((Duration)obj);
            }

            public override int GetHashCode()
            {
                return Ticks.GetHashCode();
            }

            public static bool operator ==(Duration left, Duration right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(Duration left, Duration right)
            {
                return !left.Equals(right);
            }

            public static bool operator <(Duration left, Duration right)
            {
                return left.Ticks < right.Ticks;
            }

            public static bool operator >(Duration left, Duration right)
            {
                return left.Ticks > right.Ticks;
            }

            public static bool operator <=(Duration left, Duration right)
            {
                return left.Ticks <= right.Ticks;
            }

            public static bool operator >=(Duration left, Duration right)
            {
                return left.Ticks >= right.Ticks;
            }

            public bool Equals(Duration other)
            {
                return Ticks == other.Ticks;
            }

            public int CompareTo(Duration other)
            {
                return Ticks.CompareTo(other.Ticks);
            }

            public int CompareTo(object obj)
            {
                return CompareTo((Duration)obj);
            }
        }

        public class DurationConverter : JsonConverter
        {
            public override bool CanConvert(Type objectType)
            {
                return objectType == typeof(Duration) || objectType == typeof(Duration?);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                if (reader.TokenType == JsonToken.Null)
                {
                    if (objectType != typeof(Duration?))
                        throw new InvalidDataException(string.Format("Cannot convert null value to {0}.", objectType));
                    return null;
                }

                if (reader.TokenType == JsonToken.String)
                {
                    var value = (string)reader.Value;
                    if (value == "")
                    {
                        if (objectType != typeof(Duration?))
                            throw new InvalidDataException(string.Format("Cannot convert null value to {0}.", objectType));
                        return null;
                    }
                }

                var timeSpan = serializer.Deserialize<TimeSpan>(reader);
                return new Duration(timeSpan);
            }

            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                if (value == null)
                    throw new ArgumentNullException("value");

                if (!(value is Duration))
                    throw new ArgumentException(string.Format("Unexpected value when converting. Expected {0}, got {1}.", typeof(Duration).FullName,
                        value.GetType().FullName));

                var timeSpan = new TimeSpan(((Duration)value).Ticks);
                serializer.Serialize(writer, timeSpan);
            }
        }

        public static bool DurationQueryValueConverter(string name, Duration value, bool forRange, out object objValue)
        {
            if (forRange)
            {
                objValue = value.Ticks;
                return true;
            }

            objValue = new TimeSpan(value.Ticks).ToString("c");

            return true;
        }
    }
}
