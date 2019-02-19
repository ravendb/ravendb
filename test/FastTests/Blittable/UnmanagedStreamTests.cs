using System;
using System.Collections.Generic;
using Sparrow;
using Sparrow.Json;
using Xunit;

namespace FastTests.Blittable
{
    public unsafe class UnmanagedStreamTests : NoDisposalNeeded
    {
        [Fact]
        public void EnsureSingleChunk()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                using (var newStream = ctx.GetStream(JsonOperationContext.InitialStreamSize))
                {
                    var buffer = new byte[1337];
                    new Random(1337).NextBytes(buffer);
                    fixed (byte* p = buffer)
                    {
                        for (int i = 0; i < 7; i++)
                        {
                            newStream.Write(p, buffer.Length);
                        }
                    }
                    byte* ptr;
                    int size;
                    newStream.EnsureSingleChunk(out ptr, out size);

                    buffer = new byte[buffer.Length * 7];
                    fixed (byte* p = buffer)
                    {
                        newStream.CopyTo(p);

                        Assert.Equal(size, newStream.SizeInBytes);
                        Assert.Equal(0, Memory.Compare(p, ptr, size));
                    }
                }
            }
        }

        [Fact]
        public void BulkWriteAscendingSizeTest()
        {
            //using (var unmanagedByteArrayPool = new UnmanagedBuffersPool(string.Empty))
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var allocatedMemoryList = new List<AllocatedMemoryData>();
                using (var newStream = ctx.GetStream(JsonOperationContext.InitialStreamSize))
                {
                    var totalSize = 0;
                    var rand = new Random();
                    for (var i = 1; i < 5000; i += 500)
                    {
                        var allocatedMemoryData = ctx.GetMemory(rand.Next(1, i * 7));
                        totalSize += allocatedMemoryData.SizeInBytes;
                        FillData((byte*)allocatedMemoryData.Address, allocatedMemoryData.SizeInBytes);
                        allocatedMemoryList.Add(allocatedMemoryData);
                        newStream.Write((byte*)allocatedMemoryData.Address, allocatedMemoryData.SizeInBytes);
                    }
                    var buffer = ctx.GetMemory(newStream.SizeInBytes);

                    var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                    Assert.Equal(copiedSize, newStream.SizeInBytes);

                    var curIndex = 0;
                    var curTuple = 0;
                    foreach (var allocatedMemoryData in allocatedMemoryList)
                    {
                        curTuple++;
                        for (var i = 0; i < allocatedMemoryData.SizeInBytes; i++)
                        {
                            Assert.Equal(*((byte*)buffer.Address + curIndex), *((byte*)((byte*)allocatedMemoryData.Address + i)));
                            curIndex++;
                        }
                        ctx.ReturnMemory(allocatedMemoryData);
                    }
                }
            }
        }


        [Fact]
        public void BigAlloc()
        {
            var size = 3917701;

            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var data = ctx.GetMemory(size);
                ctx.ReturnMemory(data);
            }
        }

        [Fact]
        public void BulkWriteDescendingSizeTest()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var allocatedMemory = new List<AllocatedMemoryData>();
                using (var newStream = ctx.GetStream(JsonOperationContext.InitialStreamSize))
                {
                    var rand = new Random();
                    for (var i = 5000; i > 1; i -= 500)
                    {
                        var pointer = ctx.GetMemory(rand.Next(1, i * 7));
                        FillData((byte*)pointer.Address, pointer.SizeInBytes);
                        allocatedMemory.Add(pointer);
                        newStream.Write((byte*)pointer.Address, pointer.SizeInBytes);
                    }

                    var buffer = ctx.GetMemory(newStream.SizeInBytes);

                    var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                    Assert.Equal(copiedSize, newStream.SizeInBytes);

                    var curIndex = 0;
                    foreach (var tuple in allocatedMemory)
                    {
                        for (var i = 0; i < tuple.SizeInBytes; i++)
                        {
                            Assert.Equal(*((byte*)buffer.Address + curIndex), *((byte*)((byte*)tuple.Address + i)));
                            curIndex++;
                        }
                        ctx.ReturnMemory(tuple);
                    }
                }
            }
        }

        [Fact]
        public void SingleByteWritesTest()
        {
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var allocatedMemoryList = new List<AllocatedMemoryData>();
                using (var newStream = ctx.GetStream(JsonOperationContext.InitialStreamSize))
                {
                    var rand = new Random();
                    for (var i = 1; i < 5000; i += 500)
                    {
                        var allocatedMemory = ctx.GetMemory(rand.Next(1, i * 7));
                        FillData((byte*)allocatedMemory.Address, allocatedMemory.SizeInBytes);
                        allocatedMemoryList.Add(allocatedMemory);
                        for (var j = 0; j < allocatedMemory.SizeInBytes; j++)
                        {
                            newStream.WriteByte(*((byte*)allocatedMemory.Address + j));
                        }
                    }

                    var buffer = ctx.GetMemory(newStream.SizeInBytes);

                    try
                    {
                        var copiedSize = newStream.CopyTo((byte*)buffer.Address);
                        Assert.Equal(copiedSize, newStream.SizeInBytes);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e);
                    }
                    var curIndex = 0;
                    var curTuple = 0;
                    foreach (var allocatedMemory in allocatedMemoryList)
                    {
                        curTuple++;
                        for (var i = 0; i < allocatedMemory.SizeInBytes; i++)
                        {
                            try
                            {
                                var bufferValue = *((byte*)buffer.Address + curIndex);
                                var allocatedMemoryValue = *((byte*)((byte*)allocatedMemory.Address + i));
                                Assert.Equal(bufferValue,
                                    allocatedMemoryValue);
                                curIndex++;
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine(e);
                            }
                        }
                        ctx.ReturnMemory(allocatedMemory);
                    }

                    ctx.ReturnMemory(buffer);
                }
            }
        }

        private void FillData(byte* ptr, int size)
        {
            for (var i = 0; i < size; i++)
            {
                *ptr = (byte)(i % 4);
                ptr++;
            }
        }
    }
}
