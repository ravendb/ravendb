// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SerializableStructDictionaryTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test a PersistentDiction with values that are are serializable structs.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.IO;
    using System.Net;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the where the values are serializable structs.
    /// </summary>
    [TestClass]
    public class SerializableStructDictionaryTests
    {
        /// <summary>
        /// Where the dictionary will be located.
        /// </summary>
        private const string DictionaryLocation = "SerializableStructDictionaryFixture";

        /// <summary>
        /// The dictionary we are testing.
        /// </summary>
        private PersistentDictionary<int, Bar> dictionary;

        /// <summary>
        /// Test initialization.
        /// </summary>
        [TestInitialize]
        public void Setup()
        {
            this.dictionary = new PersistentDictionary<int, Bar>(DictionaryLocation);
        }

        /// <summary>
        /// Cleanup after the test.
        /// </summary>
        [TestCleanup]
        public void Teardown()
        {
            this.dictionary.Dispose();
            if (Directory.Exists(DictionaryLocation))
            {
                Cleanup.DeleteDirectoryWithRetry(DictionaryLocation);
            }
        }

        /// <summary>
        /// Insert and retrieve a serializable object.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void InsertAndRetrieveSerializableObject()
        {
            var expected = new Bar
            {
                V = new Uri("http://www.microsoft.com"),
                W = IPAddress.Any,
                X = DateTime.Now,
                Y = Guid.NewGuid(),
                Z = new Foo
                {
                    A = SByte.MinValue,
                    B = "InsertAndRetrieveSerializableObject",
                    C = Decimal.MinusOne,
                }
            };

            this.dictionary[1] = expected;
            Bar actual = this.dictionary[1];
            Assert.AreNotSame(expected, actual);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Update a serializable object.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        public void UpdateSerializableObject()
        {
            var expected = new Bar
            {
                V = new Uri("http://www.contoso.com"),
                W = IPAddress.Broadcast,
                X = DateTime.Now,
                Y = Guid.NewGuid(),
                Z = new Foo
                {
                    A = SByte.MaxValue,
                    B = "UpdateSerializableObject",
                    C = Decimal.One,
                }
            };

            this.dictionary[Int32.MaxValue] = expected;
            expected.X = DateTime.UtcNow;
            expected.Y = null;
            this.dictionary[Int32.MaxValue] = expected;

            Bar actual = this.dictionary[Int32.MaxValue];
            Assert.AreNotSame(expected, actual);
            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// A sample serializable struct.
        /// </summary>
        [Serializable]
        internal struct Foo : IEquatable<Foo>
        {         
            /// <summary>
            /// Dummy SByte field.
            /// </summary>
            public sbyte A;

            /// <summary>
            /// Dummy string.
            /// </summary>
            public string B;
   
            /// <summary>
            /// Gets or sets the dummy decimal property.
            /// </summary>
            public decimal C { get; set; }

            /// <summary>
            /// Determine if two objects contain the same values.
            /// </summary>
            /// <param name="other">The object to compare against.</param>
            /// <returns>True if they are equal.</returns>
            public bool Equals(Foo other)
            {
                return this.A == other.A
                       && this.B == other.B
                       && this.C == other.C;
            }
        }

        /// <summary>
        /// A sample serializable struct.
        /// </summary>
        [Serializable]
        internal struct Bar : IEquatable<Bar>
        {
            /// <summary>
            /// Test Uri field.
            /// </summary>
            public Uri V;

            /// <summary>
            /// Dummy DateTime field.
            /// </summary>
            public DateTime X;

            /// <summary>
            /// Dummy guid.
            /// </summary>
            public Guid? Y;

            /// <summary>
            /// Test IPAddress field.
            /// </summary>
            private IPAddress address;

            /// <summary>
            /// Gets or sets the IPAddress field.
            /// </summary>
            public IPAddress W
            {
                get
                {
                    return this.address;
                }

                set
                {
                    this.address = value;
                }
            }

            /// <summary>
            /// Gets or sets the foo property.
            /// </summary>
            public Foo Z { get; set; }

            /// <summary>
            /// Determine if two objects contain the same values.
            /// </summary>
            /// <param name="other">The object to compare against.</param>
            /// <returns>True if they are equal.</returns>
            public bool Equals(Bar other)
            {
                return this.V == other.V
                       && this.W == other.W
                       && this.X == other.X
                       && this.Y == other.Y
                       && this.Z.Equals(other.Z);
            }
        }
    }
}