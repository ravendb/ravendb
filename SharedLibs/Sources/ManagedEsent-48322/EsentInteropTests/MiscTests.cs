//-----------------------------------------------------------------------
// <copyright file="MiscTests.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace InteropApiTests
{
    using System;
    using System.Linq;
    using System.Reflection;
    using Microsoft.Isam.Esent.Interop;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    /// <summary>
    /// Miscellaneous tests.
    /// </summary>
    [TestClass]
    public class MiscTests
    {
        /// <summary>
        /// Test calling JetFreeBuffer on a null buffer.
        /// </summary>
        [TestMethod]
        [Description("Test calling JetFreeBuffer on a null buffer")]
        [Priority(0)]
        public void FreeNullBuffer()
        {
            Api.JetFreeBuffer(IntPtr.Zero);
        }

        /// <summary>
        /// Verify that all TestMethods in this assembly have priorities.
        /// </summary>
        [TestMethod]
        [Description("Verify that all TestMethods in this assembly have priorities")]
        [Priority(1)]
        public void VerifyAllTestMethodsHavePriorities()
        {
            Assembly assembly = this.GetType().Assembly;
            VerifyAllTestMethodsHaveAttribute<PriorityAttribute>(assembly);
        }

        /// <summary>
        /// Verify that all TestMethods in this assembly have descriptions.
        /// </summary>
        [TestMethod]
        [Description("Verify that all TestMethods in this assembly have descriptions")]
        [Priority(1)]
        public void VerifyAllTestMethodsHaveDescriptions()
        {
            Assembly assembly = this.GetType().Assembly;
            VerifyAllTestMethodsHaveAttribute<DescriptionAttribute>(assembly);
        }

        /// <summary>
        /// Verify that all public methods on public types in the assembly
        /// have the [TestMethod] attribute.
        /// </summary>
        [TestMethod]
        [Description("Verify that all public methods on public types in the assembly have the [TestMethod] attribute")]
        [Priority(1)]
        public void VerifyAllPublicMethodsAreTests()
        {
            Assembly assembly = this.GetType().Assembly;
            var methods = FindPublicNonTestMethods(assembly);
            if (methods.Length > 0)
            {
                Console.WriteLine("{0} public methods have no [TestMethod] attribute", methods.Length);
                foreach (string m in methods)
                {
                    Console.WriteLine("\t{0}", m);
                }

                Assert.Fail("A public method is not a test. Missing a [TestMethod] attribute?");
            }
        }

        /// <summary>
        /// Verify that all TestMethods in an assembly have a specific attribute.
        /// If not all methods have the attribute then the names of the methods
        /// are printed and the test fails.
        /// </summary>
        /// <typeparam name="T">The required attribute.</typeparam>
        /// <param name="assembly">The assembly to look in.</param>
        private static void VerifyAllTestMethodsHaveAttribute<T>(Assembly assembly) where T : Attribute
        {
            var methods = FindTestMethodsWithoutAttribute<T>(assembly);
            if (methods.Length > 0)
            {
                Console.WriteLine("{0} methods have no {1} attribute", methods.Length, typeof(T).Name);
                foreach (string m in methods)
                {
                    Console.WriteLine("\t{0}", m);
                }

                Assert.Fail("A test method does not have a required attribute");
            }
        }

        /// <summary>
        /// Return an array of all test methods in the given assembly that do not have
        /// a specific attribute.
        /// </summary>
        /// <typeparam name="T">The required attribute.</typeparam>
        /// <param name="assembly">The assembly to look in.</param>
        /// <returns>An array of method names for test methods without the attribute.</returns>
        private static string[] FindTestMethodsWithoutAttribute<T>(Assembly assembly) where T : Attribute
        {
            return assembly.GetTypes()
                .SelectMany(
                    t => from method in t.GetMethods()
                         where method.GetCustomAttributes(true).Any(attribute => attribute is TestMethodAttribute)
                               && !method.GetCustomAttributes(true).Any(attribute => attribute is T)
                         select String.Format("{0}.{1}", method.DeclaringType, method.Name))
                .OrderBy(x => x)
                .ToArray();
        }

        /// <summary>
        /// Return an array of all public methods on [TestClass] types in the given assembly that do not have
        /// the [TestMethod] attribute. These are probably meant to be tests.
        /// </summary>
        /// <param name="assembly">The assembly to look in.</param>
        /// <returns>An array of method names for methods without the TestMethod attribute.</returns>
        private static string[] FindPublicNonTestMethods(Assembly assembly)
        {
            return assembly.GetTypes()
                .Where(type => type.IsPublic)
                .SelectMany(
                    type => from method in type.GetMethods(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance | BindingFlags.Static)
                         where !method.GetCustomAttributes(true).Any(attribute => attribute is ClassInitializeAttribute)
                               && !method.GetCustomAttributes(true).Any(attribute => attribute is ClassCleanupAttribute)
                               && !method.GetCustomAttributes(true).Any(attribute => attribute is TestInitializeAttribute)
                               && !method.GetCustomAttributes(true).Any(attribute => attribute is TestCleanupAttribute)
                               && !method.GetCustomAttributes(true).Any(attribute => attribute is TestMethodAttribute)
                     select String.Format("{0}.{1}", method.DeclaringType, method.Name))
                .OrderBy(x => x)
                .ToArray();
        }
    }
}
