using System;
using System.Linq;
using FastTests.Voron;
using FastTests.Voron.FixedSize;
using Sparrow;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Containers;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron;

public class ContainerSpaceUsageCalculationTests(ITestOutputHelper output) : StorageTest(output)
{
    // Backward compatibility for platforms without SIMD operations
    private const int UshortUec128Count = (128 / 8) / sizeof(ushort); //Vector128<ushort>.Count;
    private const int UshortUec256Count = (256 / 8) / sizeof(ushort); //Vector256<ushort>.Count;
    private const int UshortUec512Count = (512 / 8) / sizeof(ushort); //Vector512<ushort>.Count;


    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void ItemMetadataHasSizeOfUShort() => Assert.Equal(sizeof(Container.ItemMetadata), sizeof(ushort));
    
    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void VectorizedCalculateTestIsExactlyTheSameAsNonVectorized()
    {
        // If all SIMD operations are supported, then each will have at least one run
        var elementsUsed = UshortUec512Count + UshortUec256Count + UshortUec128Count + 1;

        var random = new Random(1214123);
        
        
        using (var wTx = Env.WriteTransaction())
        {
            var currentContainer = wTx.OpenContainer(nameof(VectorizedCalculateTestIsExactlyTheSameAsNonVectorized));

            for (int i = 0; i < elementsUsed; i++)
            {
                Container.Allocate(llt: wTx.LowLevelTransaction, containerId: currentContainer, size: random.Next(17, 512), out var memory);
                
                memory.Fill((byte)random.Next(0, byte.MaxValue +1));
            }
            
            wTx.Commit();
        }

        using (var rTx = Env.ReadTransaction())
        {
            var currentContainer = rTx.OpenContainer(nameof(VectorizedCalculateTestIsExactlyTheSameAsNonVectorized));
            var rootPage = rTx.LowLevelTransaction.GetPage(currentContainer);
            var rootContainer = new Container(rootPage);

            var spaceUsedFromMethod = rootContainer.SpaceUsedInItems(rootPage.Pointer, out var usedItemsFromMethod);
            var sizeUsedOneByOne = SpaceUsedInItems(rootPage, ref rootContainer, out var usedItems);
            Assert.Equal(usedItems, usedItemsFromMethod);
            Assert.Equal(sizeUsedOneByOne, spaceUsedFromMethod);
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineDataWithRandomSeed(10)]
    [InlineDataWithRandomSeed(100)]
    [InlineDataWithRandomSeed(5)]
    public void VectorizedCalculateTestIsExactlyTheSameAsNonVectorizedFuzzy(int iterations, int seed)
    {
        var random = new Random(seed);
        var elementsUsed = random.Next(1, UshortUec128Count);
        elementsUsed += UshortUec512Count * random.Next(1, random.Next(64, 256));
        elementsUsed += UshortUec256Count * random.Next(1, random.Next(64, 256));
        elementsUsed += UshortUec128Count * random.Next(1, random.Next(64, 256));
        
        // Ensure that we can remove at least one per iteration
        if (iterations > elementsUsed)
            elementsUsed = iterations;
        
        long[] containersToRemove = new long[elementsUsed];
        long rootContainerPage;
        using (var wTx = Env.WriteTransaction())
        {
            rootContainerPage = wTx.OpenContainer(nameof(VectorizedCalculateTestIsExactlyTheSameAsNonVectorized));
            for (int i = 0; i < elementsUsed; i++)
            {
                containersToRemove[i] = Container.Allocate(llt: wTx.LowLevelTransaction, containerId: rootContainerPage, size: random.Next(1, 512), out var memory);
                memory.Fill((byte)random.Next(0, byte.MaxValue +1));
            }
            
        }

        AssertSpaceUsedInItems(rootContainerPage, -1);
        
        var perIteration = Enumerable.Repeat(elementsUsed / iterations, iterations).ToArray();
        perIteration[^1] += elementsUsed - perIteration.Sum(); // Make sure we remove all elements now

        random.Shuffle(perIteration);
        random.Shuffle(containersToRemove);
        var containersToRemoveOffset = 0;

        for (int itCount = 0; itCount < perIteration.Length; itCount++)
        {
            int deleteCount = perIteration[itCount];
            var idsToRemove = containersToRemove.AsSpan(containersToRemoveOffset, deleteCount);
            containersToRemoveOffset += deleteCount;

            foreach (var currentId in idsToRemove)
            {
                using (var wTx = Env.WriteTransaction())
                {
                    Container.Delete(wTx.LowLevelTransaction, rootContainerPage, currentId);
                    wTx.Commit();
                }

                AssertSpaceUsedInItems(rootContainerPage, itCount);
            }
        }

        AssertSpaceUsedInItems(rootContainerPage, iterations);
        using (var rTx = Env.ReadTransaction())
        {
            var rootPage = rTx.LowLevelTransaction.GetPage(rootContainerPage);
            var rootContainer = new Container(rootPage);

            var hasEntries = false;

            for (int i = 0; i < rootContainer.Header.NumberOfOffsets; i++)
            {
                if (rootContainer.MetadataFor(i).IsFree == false)
                {
                    hasEntries = true; 
                    break; 
                }
            }
            
            
            Assert.Equal(hasEntries, rootContainer.HasEntries());
        }
    }

    private unsafe void AssertSpaceUsedInItems(long rootContainerPage, int iterationIdx)
    {
        using (var rTx = Env.ReadTransaction())
        {
            var rootPage = rTx.LowLevelTransaction.GetPage(rootContainerPage);
            var rootContainer = new Container(rootPage);

            var spaceUsedFromMethod = rootContainer.SpaceUsedInItems(rootPage.Pointer, out var usedItemsFromMethod);
            var sizeUsedOneByOne = SpaceUsedInItems(rootPage, ref rootContainer, out var usedItems);

            Assert.True(sizeUsedOneByOne == spaceUsedFromMethod,  $"Size | Expected: {sizeUsedOneByOne} | Actual: {spaceUsedFromMethod} | Iteration {iterationIdx}");
            
            Assert.True(usedItems == usedItemsFromMethod,  $"Count | Expected: {usedItems} | Actual: {usedItemsFromMethod} | Iteration {iterationIdx}");
        }
    }
    
    private static unsafe int SpaceUsedInItems(in Page page, ref Container container, out int usedItems)
    {
        var numberOfOffsets = container.Header.NumberOfOffsets;
        var size = 0;
        usedItems = 0;
        for (int i = 0; i < numberOfOffsets; i++)
        {
            var currentSize = container.MetadataFor(i).GetSize(page.Pointer);
            usedItems += (currentSize != 0).ToInt32();
            size += currentSize;
        }

        return size;
    }
}
