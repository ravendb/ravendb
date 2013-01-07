//-----------------------------------------------------------------------
// <copyright file="ExceptionTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.Isam.Esent;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace PixieTests
{
    /// <summary>
    /// Test the exception classes.
    /// </summary>
    [TestClass]
    public class ExceptionTests
    {
        /// <summary>
        /// Verify that the exception can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyEsentColumnNotFoundExceptionCanBeSerialized()
        {
            var originalException = new EsentColumnNotFoundException("table", "column", null);
            EsentColumnNotFoundException deserializedException = this.SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Verify that the exception can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyEsentInvalidConversionExceptionCanBeSerialized()
        {
            var originalException = new EsentInvalidConversionException("table", "column", ColumnType.AsciiText, typeof(double), null);
            EsentInvalidConversionException deserializedException = this.SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Verify that the exception can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyEsentReadOnlyExceptionCanBeSerialized()
        {
            var originalException = new EsentReadOnlyException("something");
            EsentReadOnlyException deserializedException = this.SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Verify that the exception can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyEsentSqlExecutionExceptionCanBeSerialized()
        {
            var originalException = new EsentSqlExecutionException("description");
            EsentSqlExecutionException deserializedException = this.SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Verify that the exception can be serialized and deserialized.
        /// </summary>
        [TestMethod]
        [Priority(1)]
        public void VerifyEsentSqlParseExceptionCanBeSerialized()
        {
            var originalException = new EsentSqlParseException("description");
            EsentSqlParseException deserializedException = this.SerializeDeserialize(originalException);
            Assert.AreEqual(originalException.Message, deserializedException.Message);
        }

        /// <summary>
        /// Serialize and then deserialize the given exception.
        /// </summary>
        /// <typeparam name="T">The type of the exception.</typeparam>
        /// <param name="exception">The exception to serialize.</param>
        /// <returns>The deserialized form of the exception.</returns>
        private T SerializeDeserialize<T>(T exception) where T : Exception, ISerializable
        {
            var stream = new MemoryStream();

            var formatter = new BinaryFormatter();
            formatter.Serialize(stream, exception);
            stream.Position = 0; // rewind

            var deserializedException = (T)formatter.Deserialize(stream);
            return deserializedException;
        }
    }
}