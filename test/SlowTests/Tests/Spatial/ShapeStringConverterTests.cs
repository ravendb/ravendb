using System.Globalization;
using FastTests;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Utils;
using SlowTests.Utils.Attributes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class ShapeStringConverterTests : NoDisposalNeeded
    {
        public ShapeStringConverterTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [CriticalCultures]
        public void ParseBox(CultureInfo cultureInfo)
        {
            using (CultureHelper.EnsureCulture(cultureInfo))
            {
                var converter = new ShapeStringConverter(new SpatialOptions());
                var result1 = converter.ConvertToWKT("BOX(35.6 0.0, 45.9 79.4)");
                Assert.Equal("35.6 0.0 45.9 79.4", result1);
                var result2 = converter.ConvertToWKT("BOX2D(35.6 0.0, 45.9 79.4)");
                Assert.Equal("35.6 0.0 45.9 79.4", result2);
            }
        }

        [Theory]
        [CriticalCultures]
        public void ParseGeoUriKilometers(CultureInfo cultureInfo)
        {
            using (CultureHelper.EnsureCulture(cultureInfo))
            {
                var converter = new ShapeStringConverter(new SpatialOptions() { Units = SpatialUnits.Kilometers });
                var result1 = converter.ConvertToWKT("geo:-45.8,12.5,65.0");
                Assert.Equal("POINT (12.5 -45.8)", result1);
                var result2 = converter.ConvertToWKT("geo:-45.8,12.5,65.0;u=1000");
                Assert.Equal("Circle(12.5 -45.8 d=1)", result2);
            }
        }

        [Theory]
        [CriticalCultures]
        public void ParseGeoUriMiles(CultureInfo cultureInfo)
        {
            using (CultureHelper.EnsureCulture(cultureInfo))
            {
                var converter = new ShapeStringConverter(new SpatialOptions() { Units = SpatialUnits.Miles });
                var result1 = converter.ConvertToWKT("geo:-45.8,12.5,65.0");
                Assert.Equal("POINT (12.5 -45.8)", result1);
                var result2 = converter.ConvertToWKT("geo:-45.8,12.5,65.0;u=1000");
                Assert.Equal("Circle(12.5 -45.8 d=0.621371)", result2);
            }
        }
    }
}
