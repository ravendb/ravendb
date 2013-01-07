//-----------------------------------------------------------------------
// <copyright file="ApiTestHookTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Tests for ApiTestHook
    /// </summary>
    [TestClass]
    public class ApiTestHookTests
    {
        /// <summary>
        /// The ApiTestHook constructor should set the implementation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test that the ApiTestHook constructor sets the implementation.")]
        public void VerifyApiTestHookSetsApi()
        {
            var newImpl = new JetApi();
            using (new ApiTestHook(newImpl))
            {
                Assert.AreSame(Api.Impl, newImpl);
            }
        }

        /// <summary>
        /// Disposing of the ApiTestHook should reset the implementation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Test that disposing of an ApiTestHook resets the implementation.")]
        public void VerifyDisposingApiTestHookResetsApi()
        {
            var oldImpl = Api.Impl;
            var newImpl = new JetApi();
            using (new ApiTestHook(newImpl))
            {
            }

            Assert.AreSame(Api.Impl, oldImpl);
        }
    }
}