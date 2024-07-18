using System.Runtime.Intrinsics;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class IntrinsicsTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        public void MoveMask128()
        {
            Assert.Equal(0, PortableIntrinsics.MoveMask(Vector128<byte>.Zero));
            Assert.Equal(65535, PortableIntrinsics.MoveMask(Vector128<byte>.AllBitsSet));
            Assert.Equal(32768, PortableIntrinsics.MoveMask(Vector128.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF)));
            Assert.Equal(32896, PortableIntrinsics.MoveMask(Vector128.Create(0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0xFF)));
            Assert.Equal(34848, PortableIntrinsics.MoveMask(Vector128.Create(0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0xFF)));
            Assert.Equal(32771, PortableIntrinsics.MoveMask(Vector128.Create(0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF)));
            Assert.Equal(32774, PortableIntrinsics.MoveMask(Vector128.Create(0, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF)));
            Assert.Equal(2240, PortableIntrinsics.MoveMask(Vector128.Create(0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0, 0, 0, 0xFF, 0, 0, 0, 0)));
            Assert.Equal(16384, PortableIntrinsics.MoveMask(Vector128.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0)));
            Assert.Equal(1, PortableIntrinsics.MoveMask(Vector128.Create(0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)));

            Assert.Equal(10837, PortableIntrinsics.MoveMask(Vector128.Create(0xAF, 0, 0xEF, 0, 0xFF, 0, 0xDF, 0, 0, 0xFF, 0, 0xFF, 0, 0xFF, 0, 0x01).AsByte()));
        }

        [RavenFact(RavenTestCategory.Core | RavenTestCategory.Intrinsics)]
        public void MoveMask256()
        {
            Assert.Equal(0, PortableIntrinsics.MoveMask(Vector256<byte>.Zero));
            Assert.Equal(-1, PortableIntrinsics.MoveMask(Vector256<byte>.AllBitsSet));

            Assert.Equal(1073741824, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF).AsByte()));
            Assert.Equal(1073758208, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0xFF).AsByte()));
            Assert.Equal(1077937152, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0, 0, 0xFF, 0, 0, 0, 0xFF).AsByte()));
            Assert.Equal(1073741829, PortableIntrinsics.MoveMask(Vector256.Create(0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF).AsByte()));
            Assert.Equal(1073741844, PortableIntrinsics.MoveMask(Vector256.Create(0, 0xFF, 0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF).AsByte()));
            Assert.Equal(4214784, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 0, 0, 0, 0xFF, 0, 0, 0, 0).AsByte()));
            Assert.Equal(268435456, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0).AsByte()));
            Assert.Equal(1, PortableIntrinsics.MoveMask(Vector256.Create(0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).AsByte()));

            Assert.Equal(-2147483648, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF00).AsByte()));
            Assert.Equal(-2147450880, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0xFF00, 0, 0, 0, 0, 0, 0, 0, 0xFF00).AsByte()));
            Assert.Equal(-2139092992, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0xFF00, 0, 0, 0, 0, 0, 0xFF00, 0, 0, 0, 0xFF00).AsByte()));
            Assert.Equal(-2147483638, PortableIntrinsics.MoveMask(Vector256.Create(0xFF00, 0xFF00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF00).AsByte()));
            Assert.Equal(-2147483608, PortableIntrinsics.MoveMask(Vector256.Create(0, 0xFF00, 0xFF00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF00).AsByte()));
            Assert.Equal(8429568, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0xFF00, 0xFF00, 0, 0, 0, 0xFF00, 0, 0, 0, 0).AsByte()));
            Assert.Equal(536870912, PortableIntrinsics.MoveMask(Vector256.Create(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF00, 0).AsByte()));
            Assert.Equal(2, PortableIntrinsics.MoveMask(Vector256.Create(0xFF00, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0).AsByte()));
        }
    }
}
