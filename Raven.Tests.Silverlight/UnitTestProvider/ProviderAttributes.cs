// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.
namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	static class ProviderAttributes
	{
		static ProviderAttributes()
		{
			TestClass = typeof (TestClassAttribute);
			IgnoreAttribute = typeof (IgnoreAttribute);
			ClassInitialize = typeof (ClassInitializeAttribute);
			ClassCleanup = typeof (ClassCleanupAttribute);
			TestInitialize = typeof (TestInitializeAttribute);
			TestCleanup = typeof (TestCleanupAttribute);
			DescriptionAttribute = typeof (DescriptionAttribute);
			TimeoutAttribute = typeof (TimeoutAttribute);
			OwnerAttribute = typeof (OwnerAttribute);
			ExpectedExceptionAttribute = typeof (ExpectedExceptionAttribute);
			AssemblyInitialize = typeof (AssemblyInitializeAttribute);
			AssemblyCleanup = typeof (AssemblyCleanupAttribute);
			TestMethod = typeof (TestMethodAttribute);
			Priority = typeof (PriorityAttribute);
			TestProperty = typeof (TestPropertyAttribute);
		}

		/// <summary>
		/// Gets VSTT [TestClass] attribute.
		/// </summary>
		public static Type TestClass { get; private set; }

		/// <summary>
		/// Gets VSTT [Ignore] attribute.
		/// </summary>
		public static Type IgnoreAttribute { get; private set; }

		/// <summary>
		/// Gets VSTT [ClassInitialize] attribute.
		/// </summary>
		public static Type ClassInitialize { get; private set; }

		/// <summary>
		/// Gets VSTT [Priority] attribute.
		/// </summary>
		public static Type Priority { get; private set; }

		/// <summary>
		/// Gets VSTT [ClassCleanup] attribute.
		/// </summary>
		public static Type ClassCleanup { get; private set; }

		/// <summary>
		/// Gets VSTT [TestInitialize] attribute.
		/// </summary>
		public static Type TestInitialize { get; private set; }

		/// <summary>
		/// Gets VSTT [TestCleanup] attribute.
		/// </summary>
		public static Type TestCleanup { get; private set; }

		/// <summary>
		/// Gets VSTT [Description] attribute.
		/// </summary>
		public static Type DescriptionAttribute { get; private set; }

		/// <summary>
		/// Gets VSTT [Timeout] attribute.
		/// </summary>
		public static Type TimeoutAttribute { get; private set; }

		/// <summary>
		/// Gets VSTT [Owner] attribute.
		/// </summary>
		public static Type OwnerAttribute { get; private set; }

		/// <summary>
		/// Gets VSTT [ExpectedException] attribute.
		/// </summary>
		public static Type ExpectedExceptionAttribute { get; private set; }

		/// <summary>
		/// Gets VSTT [AssemblyInitialize] attribute.
		/// </summary>
		public static Type AssemblyInitialize { get; private set; }

		/// <summary>
		/// Gets VSTT [AssemblyCleanup] attribute.
		/// </summary>
		public static Type AssemblyCleanup { get; private set; }

		/// <summary>
		/// Gets VSTT [TestMethod] attribute.
		/// </summary>
		public static Type TestMethod { get; private set; }

		/// <summary>
		/// Gets VSTT [TestProperty] attribute.
		/// </summary>
		public static Type TestProperty { get; private set; }
	}
}