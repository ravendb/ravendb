//-----------------------------------------------------------------------
// <copyright file="ExceptionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.IO;
    using System.Runtime.Serialization.Formatters.Binary;
    using Microsoft.Isam.Esent;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;
    using Rhino.Mocks.Constraints;

    /// <summary>
    /// Test the exception classes.
    /// </summary>
    [TestClass]
    public class ExceptionTests
    {
        /// <summary>
        /// Verify that creating an EsentException with a message sets the message.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that creating an EsentException with a message sets the message")]
        public void VerifyEsentExceptionConstructorSetsMessage()
        {
            var ex = new EsentException("hello");
            Assert.AreEqual("hello", ex.Message);
        }

        /// <summary>
        /// Verify that creating an EsentException with an innner exception sets
        /// the inner exception property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that creating an EsentException with an innner exception sets the inner exception property")]
        public void VerifyEsentExceptionConstructorSetsInnerException()
        {
            var ex = new EsentException("foo", new OutOfMemoryException("InnerException"));
            Assert.AreEqual("InnerException", ex.InnerException.Message);
        }

        /// <summary>
        /// Verify that the error passed into the constructor is set in the error
        /// property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the error passed into the constructor is set in the error property")]
        public void VerifyEsentErrorExceptionConstructorSetsError()
        {
            var ex = new EsentErrorException(JET_err.AccessDenied);

            Assert.AreEqual(JET_err.AccessDenied, ex.Error);
        }

        /// <summary>
        /// Verify that the error passed into the constructor is set in the error
        /// property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the error passed into the constructor is set in the error property")]
        public void VerifyEsentErrorExceptionHasMessage()
        {
            var ex = new EsentErrorException(JET_err.AccessDenied);
            Assert.IsNotNull(ex.Message);
        }

        /// <summary>
        /// Verify that an EsentErrorException can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an EsentErrorException can be serialized and deserialized")]
        public void VerifyEsentErrorExceptionSerializationPreservesError()
        {
            var originalException = new EsentErrorException(JET_err.VersionStoreOutOfMemory);

            var stream = new MemoryStream();

            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, originalException);

            stream.Position = 0; // rewind

            var deserializedException = (EsentErrorException)formatter.Deserialize(stream);
            Assert.AreEqual(originalException.Error, deserializedException.Error);
        }

        /// <summary>
        /// Verify that an EsentInvalidColumnException can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an EsentInvalidColumnException can be serialized and deserialized")]
        public void VerifyEsentInvalidColumnExceptionSerializationPreservesMessage()
        {
            var originalException = new EsentInvalidColumnException();

            var stream = new MemoryStream();

            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, originalException);

            stream.Position = 0; // rewind

            var deserializedException = (EsentInvalidColumnException)formatter.Deserialize(stream);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Verify that the exception gets the correct error description from ESENT.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the exception gets the correct error description from ESENT")]
        public void VerifyEsentErrorExceptionGetsErrorDescriptionFromSystemParameter()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                const string ExpectedDescription = "Error Description";
                Expect.Call(
                    mockApi.JetGetSystemParameter(
                        Arg<JET_INSTANCE>.Is.Anything,
                        Arg<JET_SESID>.Is.Anything,
                        Arg<JET_param>.Is.Equal(JET_param.ErrorToString),
                        ref Arg<int>.Ref(Is.Equal((int)JET_err.OutOfMemory), (int)JET_err.OutOfMemory).Dummy,
                        out Arg<string>.Out(ExpectedDescription).Dummy,
                        Arg<int>.Is.Anything))                
                    .Return((int)JET_err.Success);
                mocks.ReplayAll();

                var ex = new EsentErrorException(JET_err.OutOfMemory);
                Assert.AreEqual(ExpectedDescription, ex.ErrorDescription);
            }
        }

        /// <summary>
        /// Verify that the exception has a default error description when the
        /// call to retrieve an error description fails.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the exception has a default error description when the call to retrieve an error description fails")]
        public void VerifyEsentErrorExceptionHasDefaultErrorDescription()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            using (new ApiTestHook(mockApi))
            {
                Expect.Call(
                    mockApi.JetGetSystemParameter(
                        Arg<JET_INSTANCE>.Is.Anything,
                        Arg<JET_SESID>.Is.Anything,
                        Arg<JET_param>.Is.Equal(JET_param.ErrorToString),
                        ref Arg<int>.Ref(Is.Equal((int)JET_err.OutOfMemory), (int)JET_err.OutOfMemory).Dummy,
                        out Arg<string>.Out(null).Dummy,
                        Arg<int>.Is.Anything))
                    .Return((int)JET_err.InvalidParameter);
                mocks.ReplayAll();

                var ex = new EsentErrorException(JET_err.OutOfMemory);
                Assert.IsTrue(!String.IsNullOrEmpty(ex.ErrorDescription));
            }
        }

        /// <summary>
        /// Check that an exception is thrown when an API call fails and
        /// that it contains the right error code.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Check that an exception is thrown when an API call fails and that it contains the right error code")]
        public void EsentExceptionIsThrownOnApiError()
        {
            using (var instance = new Instance("EsentExceptionHasErrorCode"))
            {
                SetupHelper.SetLightweightConfiguration(instance);
                instance.Init();
                using (var session = new Session(instance))
                {
                    try
                    {
                        // The session shouldn't be in a transaction so this will
                        // generate an error.
                        Api.JetCommitTransaction(session, CommitTransactionGrbit.None);
                        Assert.Fail("Should have thrown an exception");
                    }
                    catch (EsentErrorException ex)
                    {
                        Assert.AreEqual(JET_err.NotInTransaction, ex.Error);
                    }
                }
            }
        }
    }
}