//-----------------------------------------------------------------------
// <copyright file="InstanceInfoConversionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Runtime.InteropServices;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test JET_INSTANCE_INFO.SetFromNative
    /// </summary>
    [TestClass]
    public class InstanceInfoConversionTests
    {
        /// <summary>
        /// The name of the instance.
        /// </summary>
        private const string InstanceName = "Instance";

        /// <summary>
        /// The name of the first database.
        /// </summary>
        private const string Database0 = "foo.edb";

        /// <summary>
        /// The name of the second database.
        /// </summary>
        private const string Database1 = "bar.edb";

        /// <summary>
        /// The instance.
        /// </summary>
        private static readonly IntPtr instance = new IntPtr(0x111);

        /// <summary>
        /// Test JET_INSTANCE_INFO.SetFromNativeUnicode.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_INSTANCE_INFO.SetFromNativeUnicode")]
        public void TestSetFromNativeUnicode()
        {
            TestSetFromNative(Marshal.StringToHGlobalUni, (m, n) => m.SetFromNativeUnicode(n));
        }

        /// <summary>
        /// Test JET_INSTANCE_INFO.SetFromNativeAscii.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test JET_INSTANCE_INFO.SetFromNativeAscii")]
        public void TestSetFromNativeAscii()
        {
            TestSetFromNative(Marshal.StringToHGlobalAnsi, (m, n) => m.SetFromNativeAscii(n));
        }

        /// <summary>
        /// Verify the szDatabases collection is read-only.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify the szDatabases collection is read-only")]
        [ExpectedException(typeof(NotSupportedException))]
        public void VerifySzDatabasesIsReadOnly()
        {
            var info = new JET_INSTANCE_INFO(JET_INSTANCE.Nil, "instance", new[] { "foo.edb" });
            info.szDatabaseFileName[0] = "bar.edb";
        }

        /// <summary>
        /// Check that a managed object has the correct members set.
        /// </summary>
        /// <param name="info">
        /// The managed object to check.
        /// </param>
        private static void CheckManaged(JET_INSTANCE_INFO info)
        {
            Assert.AreEqual(new JET_INSTANCE { Value = instance }, info.hInstanceId);
            Assert.AreEqual(InstanceName, info.szInstanceName);
            Assert.AreEqual(2, info.cDatabases);
        }

        /// <summary>
        /// Create a native object, convert it to managed and assert the conversion was sucessful.
        /// </summary>
        /// <param name="stringConverter">
        /// Method used to marshal a managed string.
        /// </param>
        /// <param name="setter">
        /// Method used to convert a native object to a managed object.
        /// </param>
        private static unsafe void TestSetFromNative(Func<string, IntPtr> stringConverter, Action<JET_INSTANCE_INFO, NATIVE_INSTANCE_INFO> setter)
        {
            JET_INSTANCE_INFO managed = new JET_INSTANCE_INFO();
            IntPtr* databases = stackalloc IntPtr[2];
            databases[0] = stringConverter(Database0);
            databases[1] = stringConverter(Database1);

            NATIVE_INSTANCE_INFO native = new NATIVE_INSTANCE_INFO
            {
                hInstanceId = instance,
                cDatabases = new IntPtr(2),
                szInstanceName = stringConverter(InstanceName),
                szDatabaseFileName = databases
            };

            setter(managed, native);
            Marshal.FreeHGlobal(native.szInstanceName);
            Marshal.FreeHGlobal(databases[0]);
            Marshal.FreeHGlobal(databases[1]);

            CheckManaged(managed);
        }
    }
}