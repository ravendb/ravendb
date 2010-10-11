//-----------------------------------------------------------------------
// <copyright file="MemoryCacheTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Linq;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the methods of the MemoryCache class
    /// </summary>
    [TestClass]
    public class MemoryCacheTests
    {
        /// <summary>
        /// The size of the buffer being cached.
        /// </summary>
        private const int BufferSize = 1024;

        /// <summary>
        /// The number of buffers being cached.
        /// </summary>
        private const int MaxBuffers = 4;

        /// <summary>
        /// The MemoryCache object being tested.
        /// </summary>
        private MemoryCache memoryCache;

        /// <summary>
        /// Initializes the fixture by creating a MemoryCache object.
        /// </summary>
        [TestInitialize]
        [Description("Setup the MemoryCacheTests fixture")]
        public void Setup()
        {
            this.memoryCache = new MemoryCache(BufferSize, MaxBuffers);           
        }

        /// <summary>
        /// Verify freeing a null buffer throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify freeing a null buffer throws an exception")]
        [ExpectedException(typeof(ArgumentNullException))]
        public void VerifyFreeOfNullThrowsException()
        {
            this.memoryCache.Free(null);
        }

        /// <summary>
        /// Verify freeing an incorrectly sized buffer throws an exception.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify freeing an incorrectly sized buffer throws an exception")]
        [ExpectedException(typeof(ArgumentOutOfRangeException))]
        public void VerifyFreeOfIncorrectSizeBufferThrowsException()
        {
            var buffer = new byte[BufferSize / 2];
            this.memoryCache.Free(buffer);
        }

        /// <summary>
        /// Verify allocating a buffer gives a non-null result.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify allocating a buffer gives a non-null result")]
        public void VerifyAllocateDoesNotReturnNull()
        {
            Assert.IsNotNull(this.memoryCache.Allocate());
        }

        /// <summary>
        /// Verify allocating a buffer gives a buffer of the correct size.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify allocating a buffer gives a buffer of the correct size")]
        public void VerifyAllocateGivesBufferOfCorrectSize()
        {
            byte[] buffer = this.memoryCache.Allocate();
            Assert.AreEqual(BufferSize, buffer.Length);
        }

        /// <summary>
        /// Verify allocating multiple buffers gives different buffers.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify allocating a buffer gives different buffers")]
        public void VerifyMultipleAllocationsReturnUniqueBuffers()
        {
            byte[] buffer1 = this.memoryCache.Allocate();
            byte[] buffer2 = this.memoryCache.Allocate();
            Assert.AreNotSame(buffer1, buffer2);
        }

        /// <summary>
        /// Allocating a buffer, freeing it and reallocating should
        /// give back the same buffer.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test that allocating a buffer, freeing it and reallocating gives back the same buffer")]
        public void VerifyAllocationLocality()
        {
            byte[] buffer = this.memoryCache.Allocate();
            this.memoryCache.Free(buffer);
            Assert.AreEqual(buffer, this.memoryCache.Allocate());
        }

        /// <summary>
        /// Allocating multiple buffers buffer, freeing them and reallocating should
        /// give back the same buffers.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test that allocating buffers, freeing them and reallocating gives back the same buffers")]
        public void VerifyMultiBufferAllocationLocality()
        {
            byte[][] buffers = (from i in Enumerable.Repeat(0, MaxBuffers) select this.memoryCache.Allocate()).ToArray();
            foreach (byte[] buffer in buffers)
            {
                this.memoryCache.Free(buffer);
            }

            byte[][] newBuffers = (from i in Enumerable.Repeat(0, MaxBuffers) select this.memoryCache.Allocate()).ToArray();
            CollectionAssert.AreEquivalent(buffers, newBuffers);
        }

        /// <summary>
        /// Have multiple threads allocate and free memory buffers.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Stress the MemoryCache using multiple threads")]
        public void MemoryCacheStressTest()
        {
            DateTime endTime = DateTime.Now + TimeSpan.FromSeconds(19);
            Thread[] threads =
                (from i in Enumerable.Range(0, MaxBuffers * 2) select new Thread(() => this.MemoryCacheStressThread(i, endTime))).ToArray();
            foreach (Thread t in threads)
            {
                t.Start();
            }

            foreach (Thread t in threads)
            {
                t.Join();
            }
        }

        /// <summary>
        /// Thread function for MemoryCache stress. This allocates, uses and frees buffers.
        /// </summary>
        /// <param name="id">The id of the thread.</param>
        /// <param name="endTime">The time this test should stop running.</param>
        private void MemoryCacheStressThread(int id, DateTime endTime)
        {
            byte marker = checked((byte)id);
            while (DateTime.Now < endTime)
            {
                for (int i = 0; i < 128; ++i)
                {
                    byte[] buffer = this.memoryCache.Allocate();
                    Assert.IsNotNull(buffer);
                    buffer[0] = marker;
                    Thread.Sleep(0);
                    Assert.AreEqual(marker, buffer[0]);
                    this.memoryCache.Free(buffer);
                }
            }
        }
    }
}
