using Sparrow.Server;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Sparrow
{
    public unsafe class ByteStringTests : NoDisposalNeeded
    {
        [Fact]
        public void Lifecycle()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None))
            {
                context.Allocate(512, out var byteString);

                Assert.Equal(512, byteString.Length);
                Assert.True(byteString.HasValue);
                Assert.True((ByteStringType.Mutable & byteString.Flags) != 0);
                Assert.True(byteString.IsMutable);
                Assert.Equal(1024, byteString._pointer->Size);

                context.Allocate(1024 - sizeof(ByteStringStorage), out var byteStringWithExactSize);

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
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None,ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringInFirstSegment);
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringWholeSegment);
                context.Allocate(1, out var byteStringNextSegment);

                long startLocation = (long)byteStringInFirstSegment._pointer;
                Assert.InRange((long)byteStringWholeSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.NotInRange((long)byteStringNextSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void ConstructionInsideWholeSegmentWithHistory()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                for (int i = 0; i < 10; i++)
                {
                    context.Allocate(ByteStringContext.MinBlockSizeInBytes * 2, out var _);
                }
            }
            using (new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringInFirstSegment);
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringWholeSegment);
                context.Allocate(1, out var byteStringNextSegment);

                long startLocation = (long)byteStringInFirstSegment._pointer;
                Assert.InRange((long)byteStringWholeSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.NotInRange((long)byteStringNextSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void ConstructionReleaseForReuseTheLeftOver()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringInFirstSegment);
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage) + 1, out var byteStringInNewSegment);
                context.Allocate((ByteStringContext.MinBlockSizeInBytes / 2) - sizeof(ByteStringStorage), out var byteStringInReusedSegment);

                long startLocation = (long)byteStringInFirstSegment._pointer;
                Assert.NotInRange((long)byteStringInNewSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.InRange((long)byteStringInReusedSegment._pointer, startLocation, startLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuse()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var byteStringInFirst);
                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var byteStringInSecond);

                long ptrLocation = (long)byteStringInFirst._pointer;
                Assert.InRange((long)byteStringInSecond._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);

                context.Release(ref byteStringInFirst);

                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var byteStringReused);

                Assert.InRange((long)byteStringReused._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);
                Assert.Equal(ptrLocation, (long)byteStringReused._pointer);

                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var byteStringNextSegment);
                Assert.NotInRange((long)byteStringNextSegment._pointer, ptrLocation, ptrLocation + ByteStringContext.MinBlockSizeInBytes);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuseAsSegment()
        {
            int allocationBlockSize = 2 * ByteStringContext.MinBlockSizeInBytes + 128 + sizeof(ByteStringStorage);
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, allocationBlockSize))
            {
                // Will be only 128 bytes left for the allocation unit.
                context.Allocate(2 * ByteStringContext.MinBlockSizeInBytes - sizeof(ByteStringStorage), out var byteStringInFirst);

                long ptrLocation = (long)byteStringInFirst._pointer;
                long nextPtrLocation = ptrLocation + byteStringInFirst._pointer->Size;

                context.Release(ref byteStringInFirst); // After the release the block should be reserved as a new segment. 

                // We use a different size to ensure we are not reusing a reuse bucket but big enough to avoid having space available. 
                context.Allocate(512, out var byteStringReused);

                Assert.InRange((long)byteStringReused._pointer, ptrLocation, ptrLocation + allocationBlockSize);
                Assert.Equal(ptrLocation, (long)byteStringReused._pointer); // We are the first in the segment.

                // This allocation will have an allocation unit size of 128 and fit into the rest of the initial segment, which should be 
                // available for an exact reuse bucket allocation. 
                context.Allocate(64, out var byteStringReusedFromBucket);

                Assert.Equal((long)byteStringReusedFromBucket._pointer, nextPtrLocation);
            }
        }

        [Fact]
        public void AllocateAndReleaseShouldReuseRepeatedly()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var first);
                long ptrLocation = (long)first._pointer;
                context.Release(ref first);

                for (int i = 0; i < 100; i++)
                {
                    context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var repeat);
                    Assert.Equal(ptrLocation, (long)repeat._pointer);
                    context.Release(ref repeat);
                }
            }
        }

#if VALIDATE
        [Fact]
        public void ValidationKeyAfterAllocateAndReleaseReuseShouldBeDifferent()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var first);
                context.Release(ref first);

                context.Allocate(ByteStringContext.MinBlockSizeInBytes / 2 - sizeof(ByteStringStorage), out var repeat);
                Assert.NotEqual(first.Key, repeat._pointer->Key);
                Assert.Equal(first.Key >> 32, repeat._pointer->Key >> 32);
                context.Release(ref repeat);
            }
        }

        [Fact]
        public void FailValidationTryingToReleaseInAnotherContext()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            using (var otherContext = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate(1, out var first);
                Assert.Throws<ByteStringValidationException>(() => otherContext.Release(ref first));
            }
        }

        [Fact]
        public void FailValidationReleasingAnAliasAfterReleasingOriginal()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                context.Allocate(1, out var first);
                var firstAlias = first;
                context.Release(ref first);

                Assert.Throws<InvalidOperationException>(() => context.Release(ref firstAlias));
            }
        }

        [Fact]
        public void DetectImmutableChangeOnValidation()
        {
            using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
            {
                ByteString value;

                Assert.Throws<ByteStringValidationException>(() =>
                {
                    using (context.From("string", ByteStringType.Immutable, out value))
                    {
                        value.Ptr[2] = (byte)'t';
                    }
                });
            }
        }

        [Fact]
        public void DetectImmutableChangeOnContextDispose()
        {
            Assert.Throws<ByteStringValidationException>(() =>
            {
                using (var context = new ByteStringContext<ByteStringDirectAllocator>(SharedMultipleUseFlag.None, ByteStringContext.MinBlockSizeInBytes))
                {
                    ByteString value;
                    using (context.From("string", ByteStringType.Immutable, out value))
                    {
                        value.Ptr[2] = (byte)'t';
                    }
                }
            });
        }
#endif

    }
}
