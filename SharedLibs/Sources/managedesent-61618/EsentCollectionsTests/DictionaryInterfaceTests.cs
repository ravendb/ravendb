// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DictionaryInterfaceTests.cs" company="Microsoft Corporation">
//   Copyright (c) Microsoft Corporation.
// </copyright>
// <summary>
//   Test IDictionary compatibility.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace EsentCollectionsTests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Isam.Esent.Collections.Generic;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test a class that implements IDictionary. This uses
    /// the sample code from MSDN to make sure the interface
    /// matches the documentation.
    /// </summary>
    [TestClass]
    public class DictionaryInterfaceTests
    {
        /// <summary>
        /// Verify SortedList implementation of IDictionary.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify SortedList implementation of IDictionary")]
        public void TestSortedList()
        {
            IDictionaryTest(new SortedList<string, string>());
        }

        /// <summary>
        /// Verify SortedDictionary implementation of IDictionary.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify SortedDictionary implementation of IDictionary")]
        public void TestSortedDictionary()
        {
            IDictionaryTest(new SortedDictionary<string, string>());
        }

        /// <summary>
        /// Verify Dictionary implementation of IDictionary.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Verify Dictionary implementation of IDictionary")]
        public void TestDictionary()
        {
            IDictionaryTest(new Dictionary<string, string>());
        }

        /// <summary>
        /// Verify PersistentDictionary implementation of IDictionary.
        /// </summary>
        [TestMethod]
        [Priority(2)]
        [Description("Verify PersistentDictionary implementation of IDictionary")]
        public void TestPersistentDictionary()
        {
            const string Directory = "IDictionary";
            using (var dictionary = new PersistentDictionary<string, string>(Directory))
            {
                IDictionaryTest(dictionary);
            }

            Cleanup.DeleteDirectoryWithRetry(Directory);
        }

        /// <summary>
        /// Test an IDictionary. This code is copied from the MSDN documentation
        /// for IDictionary.
        /// </summary>
        /// <param name="openWith">The IDictionary to test.</param>
        private static void IDictionaryTest(IDictionary<string, string> openWith)
        {
            // Add some elements to the dictionary. There are no 
            // duplicate keys, but some of the values are duplicates.
            openWith.Add("txt", "notepad.exe");
            openWith.Add("bmp", "paint.exe");
            openWith.Add("dib", "paint.exe");
            openWith.Add("rtf", "wordpad.exe");

            // The Add method throws an exception if the new key is 
            // already in the dictionary.
            try
            {
                openWith.Add("txt", "winword.exe");
                Assert.Fail("Expected an ArgumentException");
            }
            catch (ArgumentException)
            {
                Console.WriteLine("An element with Key = \"txt\" already exists.");
            }

            // The Item property is another name for the indexer, so you 
            // can omit its name when accessing elements. 
            Assert.AreEqual("wordpad.exe", openWith["rtf"]);
            Console.WriteLine(
                "For key = \"rtf\", value = {0}.",
                openWith["rtf"]);

            // The indexer can be used to change the value associated
            // with a key.
            openWith["rtf"] = "winword.exe";
            Assert.AreEqual("winword.exe", openWith["rtf"]);
            Console.WriteLine(
                "For key = \"rtf\", value = {0}.",
                openWith["rtf"]);

            // If a key does not exist, setting the indexer for that key
            // adds a new key/value pair.
            openWith["doc"] = "winword.exe";

            // The indexer throws an exception if the requested key is
            // not in the dictionary.
            try
            {
                Console.WriteLine(
                    "For key = \"tif\", value = {0}.",
                    openWith["tif"]);
                Assert.Fail("Expected KeyNotFoundException");
            }
            catch (KeyNotFoundException)
            {
                Console.WriteLine("Key = \"tif\" is not found.");
            }

            // When a program often has to try keys that turn out not to
            // be in the dictionary, TryGetValue can be a more efficient 
            // way to retrieve values.
            string value = String.Empty;
            if (openWith.TryGetValue("tif", out value))
            {
                Console.WriteLine("For key = \"tif\", value = {0}.", value);
                Assert.Fail("Expected key \"tif\" to not be found");
            }
            else
            {
                Console.WriteLine("Key = \"tif\" is not found.");
            }

            // ContainsKey can be used to test keys before inserting 
            // them.
            Assert.IsFalse(openWith.ContainsKey("ht"));
            if (!openWith.ContainsKey("ht"))
            {
                openWith.Add("ht", "hypertrm.exe");
                Console.WriteLine(
                    "Value added for key = \"ht\": {0}",
                    openWith["ht"]);
            }

            // When you use foreach to enumerate dictionary elements,
            // the elements are retrieved as KeyValuePair objects.
            Console.WriteLine();
            foreach (KeyValuePair<string, string> kvp in openWith)
            {
                Console.WriteLine(
                    "Key = {0}, Value = {1}",
                    kvp.Key,
                    kvp.Value);
            }

            // To get the values alone, use the Values property.
            ICollection<string> icoll = openWith.Values;

            // The elements of the ValueCollection are strongly typed
            // with the type that was specified for dictionary values.
            Console.WriteLine();
            foreach (string s in icoll)
            {
                Console.WriteLine("Value = {0}", s);
            }

            // To get the keys alone, use the Keys property.
            icoll = openWith.Keys;

            // The elements of the ValueCollection are strongly typed
            // with the type that was specified for dictionary values.
            Console.WriteLine();
            foreach (string s in icoll)
            {
                Console.WriteLine("Key = {0}", s);
            }

            // Use the Remove method to remove a key/value pair.
            Console.WriteLine("\nRemove(\"doc\")");
            openWith.Remove("doc");

            Assert.IsFalse(openWith.ContainsKey("doc"));
            if (!openWith.ContainsKey("doc"))
            {
                Console.WriteLine("Key \"doc\" is not found.");
            }
        }
    }
}
