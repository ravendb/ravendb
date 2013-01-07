//-----------------------------------------------------------------------
// <copyright file="ColumnValueTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Diagnostics;
    using System.Threading;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Testing ColumnValue objects.
    /// </summary>
    [TestClass]
    public class ColumnValueTests
    {
        /// <summary>
        /// Test the ValuesAsObject method of a Bool column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test BoolColumnValue.ValueAsObject")]
        public void TestBoolValueAsObject()
        {
            TestValueAsObjectForStruct(new BoolColumnValue(), true);
            TestValueAsObjectForStruct(new BoolColumnValue(), false);
        }

        /// <summary>
        /// Test the ValuesAsObject method of a Byte column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test ByteColumnValue.ValueAsObject")]
        public void TestByteValueAsObject()
        {
            var value = new ByteColumnValue();
            for (byte i = Byte.MinValue; i < Byte.MaxValue; ++i)
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an Int16 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int16ColumnValue.ValueAsObject")]
        public void TestInt16ValueAsObject()
        {
            var value = new Int16ColumnValue();
            for (short i = Int16.MinValue; i < Int16.MaxValue; ++i)
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an UInt16 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt16ColumnValue.ValueAsObject")]
        public void TestUInt16ValueAsObject()
        {
            var value = new UInt16ColumnValue();
            for (ushort i = UInt16.MinValue; i < UInt16.MaxValue; ++i)
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an Int32 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int32ColumnValue.ValueAsObject")]
        public void TestInt32ValueAsObject()
        {
            var value = new Int32ColumnValue();
            foreach (int i in new[] { Int32.MinValue, Int32.MinValue + 1, -1, 0, 1, Any.Int32, 65521, Int32.MaxValue - 1, Int32.MaxValue })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an UInt32 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt32ColumnValue.ValueAsObject")]
        public void TestUInt32ValueAsObject()
        {
            var value = new UInt32ColumnValue();
            foreach (uint i in new[] { UInt32.MinValue, 1U, 2U, Any.UInt32, 65521U, UInt32.MaxValue - 1, UInt32.MaxValue })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an Int64 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int64ColumnValue.ValueAsObject")]
        public void TestInt64ValueAsObject()
        {
            var value = new Int64ColumnValue();
            foreach (int i in new[] { Int64.MinValue, Int64.MinValue + 1, -1, 0, 1, Any.Int64, 65521, Int64.MaxValue - 1, Int64.MaxValue })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an UInt64 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test UInt64ColumnValue.ValueAsObject")]
        public void TestUInt64ValueAsObject()
        {
            var value = new UInt64ColumnValue();
            foreach (uint i in new[] { UInt64.MinValue, 1U, 2U, Any.UInt64, 65521U, UInt64.MaxValue - 1, UInt64.MaxValue })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of a float column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test FloatColumnValue.ValueAsObject")]
        public void TestFloatValueAsObject()
        {
            var value = new FloatColumnValue();
            foreach (float i in new[] { Single.MinValue, Single.MaxValue, -1, 0, 1, Any.Float, Single.Epsilon, Single.NegativeInfinity, Single.PositiveInfinity, Single.NaN })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of a double column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DoubleColumnValue.ValueAsObject")]
        public void TestDoubleValueAsObject()
        {
            var value = new DoubleColumnValue();
            foreach (double i in
                new[] { Double.MinValue, Double.MaxValue, -1, 0, 1, Any.Double, Double.Epsilon, Double.NegativeInfinity, Double.PositiveInfinity, Double.NaN, Math.PI, Math.E })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of a DateTime column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test DateTimeColumnValue.ValueAsObject")]
        public void TestDateTimeValueAsObject()
        {
            var value = new DateTimeColumnValue();
            foreach (DateTime i in new[] { DateTime.MinValue, DateTime.MaxValue, Any.DateTime, DateTime.Now, DateTime.UtcNow, DateTime.Today })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of a Guid column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test GuidColumnValue.ValueAsObject")]
        public void TestGuidValueAsObject()
        {
            var value = new GuidColumnValue();
            foreach (Guid i in new[] { Guid.Empty, Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() })
            {
                TestValueAsObjectForStruct(value, i);
            }
        }

        /// <summary>
        /// Test the ValuesAsObject method of an string column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test StringColumnValue.ValueAsObject")]
        public void TestStringValueAsObject()
        {
            var instance = new StringColumnValue { Value = "hello" };
            Assert.AreEqual("hello", instance.ValueAsObject);
        }

        /// <summary>
        /// Test the ValuesAsObject method of an bytes column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test BytesColumnValue.ValueAsObject")]
        public void TestBytesValueAsObject()
        {
            byte[] data = Any.Bytes;
            var instance = new BytesColumnValue { Value = data };
            Assert.AreEqual(data, instance.ValueAsObject);
        }

        /// <summary>
        /// Test the ToString() method of an Int32 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int32ColumnValue.ToString()")]
        public void TestInt32ColumnValueToString()
        {
            var instance = new Int32ColumnValue { Value = 5 };
            Assert.AreEqual("5", instance.ToString());
        }

        /// <summary>
        /// Test the ToString() method of a string column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test StringColumnValue.ToString()")]
        public void TestStringColumnValueToString()
        {
            var instance = new StringColumnValue { Value = "foo" };
            Assert.AreEqual("foo", instance.ToString());
        }

        /// <summary>
        /// Test the ToString() method of a GUID column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test GuidColumnValue.ToString()")]
        public void TestGuidColumnValueToString()
        {
            Guid guid = Guid.NewGuid();
            var instance = new GuidColumnValue { Value = guid };
            Assert.AreEqual(guid.ToString(), instance.ToString());
        }

        /// <summary>
        /// Test the ToString() method of a Bytes column value with a null value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test BytesColumnValue.ToString() with a null value")]
        public void TestNullBytesColumnValueToString()
        {
            var instance = new BytesColumnValue { Value = null };
            Assert.AreEqual(String.Empty, instance.ToString());
        }

        /// <summary>
        /// Test the ToString() method of a Bytes column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test BytesColumnValue.ToString()")]
        public void TestBytesColumnValueToString()
        {
            var instance = new BytesColumnValue { Value = BitConverter.GetBytes(0x1122334455667788UL) };
            Assert.AreEqual("88-77-66-55-44-33-22-11", instance.ToString());
        }

        /// <summary>
        /// Test value boxing with multiple threads.
        /// </summary>
        [TestMethod]
        [Priority(3)]
        [Description("Test value boxing with multiple threads")]
        public void TestValueBoxingWithMultipleThreads()
        {
            var test = new MultiThreadingBoxingTest();
            test.RunTest();
        }

        /// <summary>
        /// Test the ValueAsObject method for structure types.
        /// </summary>
        /// <typeparam name="T">The structure type.</typeparam>
        /// <param name="columnValue">The column value.</param>
        /// <param name="value">The value to set.</param>
        private static void TestValueAsObjectForStruct<T>(ColumnValueOfStruct<T> columnValue, T value) where T : struct, IEquatable<T>
        {
            columnValue.Value = value;
            object o1 = columnValue.ValueAsObject;
            Assert.AreEqual(o1, value);

            columnValue.Value = null;
            Assert.IsNull(columnValue.ValueAsObject);

            columnValue.Value = value;
            object o2 = columnValue.ValueAsObject;
            Assert.AreEqual(o1, o2);
            Assert.AreSame(o1, o2);

            columnValue.Value = default(T);
            Assert.AreEqual(columnValue.ValueAsObject, default(T));
        }

        /// <summary>
        /// A test that boxes values from multiple threads.
        /// </summary>
        private sealed class MultiThreadingBoxingTest
        {
            /// <summary>
            /// Number of test iterations to run.
            /// </summary>
            private const int N =
#if DEBUG
                100000;
#else
                10000000;
#endif

            /// <summary>
            /// Signal that is set to start the test threads.
            /// </summary>
            private readonly ManualResetEvent startEvent = new ManualResetEvent(false);

            /// <summary>
            /// Signal that is set when the last test thread finishes.
            /// </summary>
            private readonly ManualResetEvent stopEvent = new ManualResetEvent(false);

            /// <summary>
            /// Number of currently active threads.
            /// </summary>
            private int activeThreads;

            /// <summary>
            /// Run the test.
            /// </summary>
            public void RunTest()
            {
                Int32ColumnValue columnValue = new Int32ColumnValue();
                columnValue.Value = 0;
                Assert.AreEqual(0, columnValue.ValueAsObject);

                Thread[] threads = new Thread[Environment.ProcessorCount * 2];
                for (int i = 0; i < threads.Length; ++i)
                {
                    threads[i] = new Thread(this.ThreadProc);
                    threads[i].Start(new Random(i));
                    this.activeThreads++;
                }

                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();

                Stopwatch stopwatch = Stopwatch.StartNew();
                this.startEvent.Set();
                this.stopEvent.WaitOne();
                stopwatch.Stop();

                foreach (Thread t in threads)
                {
                    t.Join();
                }

                Console.WriteLine("Performed {0:N0} operations on {1} threads in {2}", N, threads.Length, stopwatch.Elapsed);
            }

            /// <summary>
            /// Thread proc. This boxes random values.
            /// </summary>
            /// <param name="parameter">The random number generator to use.</param>
            private void ThreadProc(object parameter)
            {
                Random rand = (Random)parameter;
                Int32ColumnValue columnValue = new Int32ColumnValue();

                this.startEvent.WaitOne();
                for (int i = 0; i < N; ++i)
                {
                    int x = rand.Next();
                    columnValue.Value = x;
                    object obj = columnValue.ValueAsObject;
                    Assert.AreEqual(x, obj);
                }

                if (0 == Interlocked.Decrement(ref this.activeThreads))
                {
                    this.stopEvent.Set();
                }
            }
        }
    }
}