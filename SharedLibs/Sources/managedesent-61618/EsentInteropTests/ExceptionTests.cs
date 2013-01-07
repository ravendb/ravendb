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
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Implementation;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Rhino.Mocks;
    using Rhino.Mocks.Constraints;

    /// <summary>
    /// Test the exception classes.
    /// </summary>
    [TestClass]
    public partial class ExceptionTests
    {
        /// <summary>
        /// Verify that the error passed into the constructor is set in the error
        /// property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the error passed into the constructor is set in the error property")]
        public void VerifyEsentErrorExceptionConstructorSetsError()
        {
            var ex = new EsentErrorException(String.Empty, JET_err.AccessDenied);

            Assert.AreEqual(JET_err.AccessDenied, ex.Error);
        }

        /// <summary>
        /// Verify that the error passed into the constructor is set in the error
        /// property.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that the error passed into the constructor is set in the error property")]
        public void VerifyEsentErrorExceptionSetsMessage()
        {
            string expected = Any.String;
            var ex = new EsentErrorException(expected, JET_err.AccessDenied);
            Assert.AreEqual(expected, ex.Message);
        }

        /// <summary>
        /// Verify that an EsentErrorException can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify that an EsentErrorException can be serialized and deserialized")]
        public void VerifyEsentErrorExceptionSerializationPreservesError()
        {
            var originalException = new EsentErrorException("description", JET_err.VersionStoreOutOfMemory);
            var deserializedException = SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Error, deserializedException.Error);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
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
            var deserializedException = SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Create every error exception.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Create every error exception")]
        public void CreateAllErrorExceptions()
        {
            int i = 0;
            foreach (JET_err err in Enum.GetValues(typeof(JET_err)))
            {
                if (JET_err.Success != err)
                {
                    EsentErrorException ex = EsentExceptionHelper.JetErrToException(err);
                    Assert.IsNotNull(ex);
                    Assert.AreEqual(err, ex.Error);
                    Assert.IsNotNull(ex.Message);
                    Assert.AreNotEqual(String.Empty, ex.Message);

                    EsentErrorException deserialized = SerializeDeserialize(ex);
                    Assert.AreEqual(err, deserialized.Error);
                    Assert.AreEqual(ex.Message, deserialized.Message);
                    i++;
                }
            }

            Console.WriteLine("Created {0} different error exceptions", i);
        }

        /// <summary>
        /// Verify that the exception gets the correct error description from ESENT when the eror is unknown
        /// </summary>
        [TestMethod]
        [Priority(1)]
        [Description("Verify that the exception gets the correct error description from ESENT when the error is unknown")]
        public void VerifyUnknownEsentErrorExceptionGetsErrorDescriptionFromSystemParameter()
        {
            var mocks = new MockRepository();
            var mockApi = mocks.StrictMock<IJetApi>();
            var rawErrorValue = new IntPtr(-9999);
            using (new ApiTestHook(mockApi))
            {
                const string ExpectedDescription = "Error Description";
                Expect.Call(
                    mockApi.JetGetSystemParameter(
                        Arg<JET_INSTANCE>.Is.Anything,
                        Arg<JET_SESID>.Is.Anything,
                        Arg<JET_param>.Is.Equal(JET_param.ErrorToString),
                        ref Arg<IntPtr>.Ref(Is.Equal(rawErrorValue), rawErrorValue).Dummy,
                        out Arg<string>.Out(ExpectedDescription).Dummy,
                        Arg<int>.Is.Anything))                
                    .Return((int)JET_err.Success);
                mocks.ReplayAll();

                var ex = EsentExceptionHelper.JetErrToException((JET_err)(-9999));
                Assert.AreEqual(ExpectedDescription, ex.Message);
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
            var rawErrorValue = new IntPtr(-9999);
            using (new ApiTestHook(mockApi))
            {
                Expect.Call(
                    mockApi.JetGetSystemParameter(
                        Arg<JET_INSTANCE>.Is.Anything,
                        Arg<JET_SESID>.Is.Anything,
                        Arg<JET_param>.Is.Equal(JET_param.ErrorToString),
                        ref Arg<IntPtr>.Ref(Is.Equal(rawErrorValue), rawErrorValue).Dummy,
                        out Arg<string>.Out(null).Dummy,
                        Arg<int>.Is.Anything))
                    .Return((int)JET_err.InvalidParameter);
                mocks.ReplayAll();

                var ex = EsentExceptionHelper.JetErrToException((JET_err)(-9999));
                Assert.IsTrue(!String.IsNullOrEmpty(ex.Message));
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
                    catch (EsentNotInTransactionException ex)
                    {
                        Assert.AreEqual(JET_err.NotInTransaction, ex.Error);
                    }
                }
            }
        }

        /// <summary>
        /// Serialize an object to an in-memory stream then deserialize it.
        /// </summary>
        /// <typeparam name="T">The type to serialize.</typeparam>
        /// <param name="obj">The object to serialize.</param>
        /// <returns>A deserialized copy of the object.</returns>
        private static T SerializeDeserialize<T>(T obj)
        {
            using (var stream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(stream, obj);

                stream.Position = 0;
                return (T)formatter.Deserialize(stream);
            }
        }
    }
}