//-----------------------------------------------------------------------
// <copyright file="ColumnValueTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Testing ColumnValue objects.
    /// </summary>
    [TestClass]
    public class ColumnValueTests
    {
        /// <summary>
        /// Test the ValuesAsObject method of an Int64 column value.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test Int64ColumnValue.ValueAsObject")]
        public void TestInt64ValueAsObject()
        {
            var instance = new Int64ColumnValue { Value = 99 };
            Assert.AreEqual(99L, instance.ValueAsObject);
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
    }
}
