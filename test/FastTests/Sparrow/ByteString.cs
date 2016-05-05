//#define VALIDATE

using Sparrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class ByteString
    {
        [Fact]
        public void Lifecycle()
        {
            using (var context = new ByteStringContext())
            {
                var byteString = context.Allocate(512);

                Assert.Equal(512, byteString.Length);
                Assert.True(byteString.HasValue);
                Assert.True((ByteStringType.Mutable & byteString.Flags) != 0);
                Assert.True(byteString.IsMutable);
                Assert.Equal(1024, byteString._pointer->Size);

                var byteStringWithExactSize = context.Allocate(1024 - sizeof(ByteStringStorage));

                Assert.True(byteStringWithExactSize.HasValue);
                Assert.Equal(1024 - sizeof(ByteStringStorage), byteStringWithExactSize.Length);
                Assert.True((ByteStringType.Mutable & byteStringWithExactSize.Flags) != 0);
                Assert.True(byteStringWithExactSize.IsMutable);
                Assert.Equal(1024, byteStringWithExactSize._pointer->Size);

                context.Release(ref byteString);
                Assert.False(byteString.HasValue);
                Assert.True(byteString._pointer == null);
            }
        }

        [Fact]
        public void ConstructionInsideWholeSegment()
        {
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var byteStringInFirstSegment = context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage));
                var byteStringWholeSegment = context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage));
                var byteStringNextSegment = context.Allocate(1);

                long startLocation = (long)byteStringInFirstSegment._pointer;
                Assert.InRange((long)byteStringWholeSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.NotInRange((long)byteStringNextSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void ConstructionReleaseForReuseTheLeftOver()
        {
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var byteStringInFirstSegment = context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage));
                var byteStringInNewSegment = context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage) + 1);
                var byteStringInReusedSegment = context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage));

                long startLocation = (long)byteStringInFirstSegment._pointer;
                Assert.NotInRange((long)byteStringInNewSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.InRange((long)byteStringInReusedSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuse()
        {
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var byteStringInFirst = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                var byteStringInSecond = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));

                long ptrLocation = (long)byteStringInFirst._pointer;
                Assert.InRange((long)byteStringInSecond._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);

                context.Release(ref byteStringInFirst);

                var byteStringReused = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));

                Assert.InRange((long)byteStringReused._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.Equal(ptrLocation, (long)byteStringReused._pointer);

                var byteStringNextSegment = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                Assert.NotInRange((long)byteStringNextSegment._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuseAsSegment()
        {
            int allocationBlockSize = 2 * ByteStringContext.MinBlockSizeInBytes + 128 + sizeof(ByteStringStorage);
            using (var context = new ByteStringContext(allocationBlockSize))
            {
                // Will be only 128 bytes left for the allocation unit.
                var byteStringInFirst = context.Allocate(2 * ByteStringContext.MinBlockSizeInBytes - sizeof(ByteStringStorage));          

                long ptrLocation = (long)byteStringInFirst._pointer;
                long nextPtrLocation = ptrLocation + byteStringInFirst._pointer->Size;

                context.Release(ref byteStringInFirst); // After the release the block should be reserved as a new segment. 

                // We use a different size to ensure we are not reusing a reuse bucket but big enough to avoid having space available. 
                var byteStringReused = context.Allocate(512);

                Assert.InRange((long)byteStringReused._pointer, ptrLocation, ptrLocation + allocationBlockSize);
                Assert.Equal(ptrLocation, (long)byteStringReused._pointer); // We are the first in the segment.

                // This allocation will have an allocation unit size of 128 and fit into the rest of the initial segment, which should be 
                // available for an exact reuse bucket allocation. 
                var byteStringReusedFromBucket = context.Allocate(64);

                Assert.Equal((long)byteStringReusedFromBucket._pointer, nextPtrLocation);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuseRepeatedly()
        {
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var first = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                long ptrLocation = (long)first._pointer;
                context.Release(ref first);

                for (int i = 0; i < 100; i++)
                {
                    var repeat = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                    Assert.Equal(ptrLocation, (long)repeat._pointer);
                    context.Release(ref repeat);
                }
            }
        }

        [SkippableFact]
        public void ValidationKeyAfterAllocateAndReleaseReuseShouldBeDifferent()
        {
#if VALIDATE
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var first = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                context.Release(ref first);

                var repeat = context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage));
                Assert.NotEqual(first.Key, repeat._pointer->Key);
                Assert.Equal(first.Key >> 32, repeat._pointer->Key >> 32);
                context.Release(ref repeat);
            }
#else
            throw new SkipTestException("Compile with the 'VALIDATE' compiler directive to create binaries compatible with validation");
#endif
        }

        [SkippableFact]
        public void FailValidationTryingToReleaseInAnotherContext()
        {
#if VALIDATE
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            using (var otherContext = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var first = context.Allocate(1);
                Assert.Throws<InvalidOperationException>(() => otherContext.Release(ref first));
            }
#else
            throw new SkipTestException("Compile with the 'VALIDATE' compiler directive to create binaries compatible with validation");
#endif
        }

        [SkippableFact]
        public void FailValidationReleasingAnAliasAfterReleasingOriginal()
        {
#if VALIDATE
            using (var context = new ByteStringContext(ByteStringContext.MinBlockSizeInBytes))
            {
                var first = context.Allocate(1);
                var firstAlias = first;
                context.Release(ref first);

                Assert.Throws<InvalidOperationException>(() => context.Release(ref firstAlias));
            }
#else
            throw new SkipTestException("Compile with the 'VALIDATE' compiler directive to create binaries compatible with validation");
#endif
        }
    }
}
