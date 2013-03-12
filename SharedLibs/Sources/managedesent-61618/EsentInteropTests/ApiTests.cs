//-----------------------------------------------------------------------
// <copyright file="ApiTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;

    /// <summary>
    /// Test the Api class functionality which wraps the IJetApi
    /// implementation.
    /// </summary>
    [TestClass]
    public class ApiTests
    {
        /// <summary>
        /// Mock object repository.
        /// </summary>
        private MockRepository mocks;

        /// <summary>
        /// The saved API, replaced when finished.
        /// </summary>
        private IJetApi savedImpl;

        /// <summary>
        /// Setup the mock object repository.
        /// </summary>
        [TestInitialize]
        [Description("Initialization for ApiTests.")]
        public void Setup()
        {
            this.savedImpl = Api.Impl;
            this.mocks = new MockRepository();
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        [Description("Cleanup for ApiTests.")]
        public void Teardown()
        {
            Api.Impl = this.savedImpl;
        }

        /// <summary>
        /// Verify that the internal IJetApi has a default implementation.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the internal IJetApi has a default implementation.")]
        public void VerifyApiHasDefaultImplementation()
        {
            Assert.IsNotNull(Api.Impl);
            Assert.IsInstanceOfType(Api.Impl, typeof(JetApi));
        }

        /// <summary>
        /// Verify that the internal IJetApi can be replaced with a different object.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the internal IJetApi can be replaced with a different object.")]
        public void VerifyJetApiImplementationCanBeChanged()
        {
            var jetApi = this.mocks.StrictMock<IJetApi>();
            Api.Impl = jetApi;

            Expect.Call(
                jetApi.JetSetCurrentIndex(JET_SESID.Nil, JET_TABLEID.Nil, String.Empty))
                .IgnoreArguments()
                .Return(1);
            this.mocks.ReplayAll();

            Api.JetSetCurrentIndex(JET_SESID.Nil, JET_TABLEID.Nil, Any.String);

            this.mocks.VerifyAll();
        }

        /// <summary>
        /// Verify that an error returned from the IJetApi implementation
        /// causes an exception to be thrown.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that an error returned from the IJetApi implementation causes an exception to be thrown.")]
        [ExpectedException(typeof(EsentOutOfMemoryException))]
        public void VerifyErrorFromJetApiImplementationGeneratesException()
        {
            var jetApi = this.mocks.Stub<IJetApi>();
            Api.Impl = jetApi;

            SetupResult.For(
                jetApi.JetSetCurrentIndex(JET_SESID.Nil, JET_TABLEID.Nil, String.Empty))
                .IgnoreArguments()
                .Return((int)JET_err.OutOfMemory);
            this.mocks.ReplayAll();

            Api.JetSetCurrentIndex(JET_SESID.Nil, JET_TABLEID.Nil, Any.String);
        }

        /// <summary>
        /// Verify that the ErrorHandler event is invoked when an error is encountered.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the ErrorHandler event is invoked when an error is encountered.")]
        public void VerifyErrorHandlerIsInvokedOnException()
        {
            var jetApi = this.mocks.Stub<IJetApi>();
            Api.Impl = jetApi;

            SetupResult.For(
                jetApi.JetBeginTransaction(JET_SESID.Nil))
                .IgnoreArguments()
                .Return((int)JET_err.TransTooDeep);
            this.mocks.ReplayAll();

            bool eventWasCalled = false;
            JET_err error = JET_err.Success;
            Api.ErrorHandler handler = errArg =>
                {
                    eventWasCalled = true;
                    error = errArg;
                };

            try
            {
                Api.HandleError += handler;
                Api.JetBeginTransaction(JET_SESID.Nil);
            }
            catch (EsentErrorException)
            {
            }

            Api.HandleError -= handler;
            Assert.IsTrue(eventWasCalled);
            Assert.AreEqual(JET_err.TransTooDeep, error);
        }

        /// <summary>
        /// Verify that the ExceptionHandler event can wrap exceptions.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the ExceptionHandler event can wrap exceptions.")]
        public void VerifyExceptionHandlerCanWrapExceptions()
        {
            var jetApi = this.mocks.Stub<IJetApi>();
            Api.Impl = jetApi;

            SetupResult.For(
                jetApi.JetBeginTransaction(JET_SESID.Nil))
                .IgnoreArguments()
                .Return((int)JET_err.TransTooDeep);
            this.mocks.ReplayAll();

            Api.ErrorHandler handler = ex =>
                {
                    throw new InvalidOperationException("test");
                };

            try
            {
                Api.HandleError += handler;
                Api.JetBeginTransaction(JET_SESID.Nil);
                Assert.Fail("Expected an invalid operation exception");
            }
            catch (InvalidOperationException)
            {
            }

            Api.HandleError -= handler;
        }
    }
}