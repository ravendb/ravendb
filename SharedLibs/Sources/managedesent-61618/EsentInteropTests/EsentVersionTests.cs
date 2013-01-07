//-----------------------------------------------------------------------
// <copyright file="EsentVersionTests.cs" company="Microsoft Corporation">
// Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.Isam.Esent.Interop.Server2003;
    using Microsoft.Isam.Esent.Interop.Vista;
    using Microsoft.Isam.Esent.Interop.Windows7;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Test the static version class
    /// </summary>
    [TestClass]
    public class EsentVersionTests
    {
        /// <summary>
        /// Print the current version of Esent (for debugging).
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Print the current version of Esent (for debugging)")]
        public void PrintVersion()
        {
            if (EsentVersion.SupportsServer2003Features)
            {
                Console.WriteLine("SupportsServer2003Features");    
            }

            if (EsentVersion.SupportsVistaFeatures)
            {
                Console.WriteLine("SupportsVistaFeatures");
            }

            if (EsentVersion.SupportsWindows7Features)
            {
                Console.WriteLine("SupportsWindows7Features");
            }

            if (EsentVersion.SupportsUnicodePaths)
            {
                Console.WriteLine("SupportsUnicodePaths");
            }

            if (EsentVersion.SupportsLargeKeys)
            {
                Console.WriteLine("SupportsLargeKeys");
            }
        }

        /// <summary>
        /// If Windows 7 is supported then older features must be 
        /// supported too.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("If Windows 7 is supported then older features must be supported too")]
        public void VerifyWindows7FeaturesIncludesOlderFeatures()
        {
            if (EsentVersion.SupportsWindows7Features)
            {
                Assert.IsTrue(EsentVersion.SupportsServer2003Features);
                Assert.IsTrue(EsentVersion.SupportsVistaFeatures);
                Assert.IsTrue(EsentVersion.SupportsUnicodePaths);
                Assert.IsTrue(EsentVersion.SupportsLargeKeys);
            }
        }

        /// <summary>
        /// If Windows Vista is supported then older features must be 
        /// supported too.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("If Windows Vista is supported then older features must be supported too")]
        public void VerifyWindowsVistaFeaturesIncludesOlderFeatures()
        {
            if (EsentVersion.SupportsVistaFeatures)
            {
                Assert.IsTrue(EsentVersion.SupportsServer2003Features);
                Assert.IsTrue(EsentVersion.SupportsUnicodePaths);
                Assert.IsTrue(EsentVersion.SupportsLargeKeys);
            }
        }

        /// <summary>
        /// Prints a list of all the Jet APIs.
        /// </summary>
        [TestMethod]
        [Priority(0)]
        [Description("Prints a list of all the Jet APIs")]
        public void ListAllApis()
        {            
            Console.WriteLine("Api");
            int totalApis = PrintJetApiNames(typeof(Api));
            Console.WriteLine("Server2003Api");
            totalApis += PrintJetApiNames(typeof(Server2003Api));
            Console.WriteLine("VistaApi");
            totalApis += PrintJetApiNames(typeof(VistaApi));
            Console.WriteLine("Windows7Api");
            totalApis += PrintJetApiNames(typeof(Windows7Api));
            Console.WriteLine("Total APIs: {0}", totalApis);
        }

        /// <summary>
        /// Prints a sorted list of the Jet apis in the given type.
        /// </summary>
        /// <param name="type">The type to inspect.</param>
        /// <returns>The number of APIs found in the type.</returns>
        private static int PrintJetApiNames(Type type)
        {
            int numApisFound = 0;
            foreach (string method in GetJetApiNames(type).OrderBy(x => x).Distinct())
            {
                Console.WriteLine("\t{0}", method);
                numApisFound++;
            }

            return numApisFound;
        }

        /// <summary>
        /// Returns the names of all the static methods in the given type
        /// that start with 'Jet'.
        /// </summary>
        /// <param name="type">The type to look at.</param>
        /// <returns>
        /// An enumeration of all the static methods in the type that
        /// start with 'Jet'.
        /// </returns>
        private static IEnumerable<string> GetJetApiNames(Type type)
        {
            foreach (MemberInfo member in type.GetMembers(BindingFlags.Public | BindingFlags.Static))
            {
                if (member.Name.StartsWith("Jet") && (member.MemberType == MemberTypes.Method))
                {
                    yield return member.Name;
                }
            }            
        }
    }
}