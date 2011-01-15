namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections.Generic;
	using System.Reflection;
	using Microsoft.Silverlight.Testing.Harness;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;

	public class TestClass : ITestClass
	{
		readonly IDictionary<Methods, LazyMethodInfo> methods;
		readonly Type type;
		ICollection<ITestMethod> tests;
		bool testsLoaded;

		TestClass(IAssembly assembly)
		{
			tests = new List<ITestMethod>();

			methods = new Dictionary<Methods, LazyMethodInfo>(4);
			methods[Methods.ClassCleanup] = null;
			methods[Methods.ClassInitialize] = null;
			methods[Methods.TestCleanup] = null;
			methods[Methods.TestInitialize] = null;

			Assembly = assembly;
		}

		public TestClass(IAssembly assembly, Type testClassType) : this(assembly)
		{
			type = testClassType;

			if (type == null)
			{
				throw new ArgumentNullException("testClassType");
			}

			methods[Methods.ClassCleanup] = new LazyMethodInfo(type, ProviderAttributes.ClassCleanup);
			methods[Methods.ClassInitialize] = new LazyMethodInfo(type, ProviderAttributes.ClassInitialize);
			methods[Methods.TestCleanup] = new LazyMethodInfo(type, ProviderAttributes.TestCleanup);
			methods[Methods.TestInitialize] = new LazyMethodInfo(type, ProviderAttributes.TestInitialize);
		}

		public IAssembly Assembly { get; protected set; }

		public Type Type
		{
			get { return type; }
		}

		public string Name
		{
			get { return type.Name; }
		}

		public ICollection<ITestMethod> GetTestMethods()
		{
			if (!testsLoaded)
			{
				var test_methods = ReflectionUtility.GetMethodsWithAttribute(
						type, ProviderAttributes.TestMethod);
				tests = new List<ITestMethod>(test_methods.Count);
				foreach (MethodInfo method in test_methods)
				{
					tests.Add(new TestMethod(method));
				}
				testsLoaded = true;
			}
			return tests;
		}

		public bool Ignore
		{
			get { return ReflectionUtility.HasAttribute(type, ProviderAttributes.IgnoreAttribute); }
		}

		public MethodInfo TestInitializeMethod
		{
			get { return methods[Methods.TestInitialize] == null ? null : methods[Methods.TestInitialize].GetMethodInfo(); }
		}

		public MethodInfo TestCleanupMethod
		{
			get { return methods[Methods.TestCleanup] == null ? null : methods[Methods.TestCleanup].GetMethodInfo(); }
		}

		public MethodInfo ClassInitializeMethod
		{
			get { return methods[Methods.ClassInitialize] == null ? null : methods[Methods.ClassInitialize].GetMethodInfo(); }
		}

		public MethodInfo ClassCleanupMethod
		{
			get { return methods[Methods.ClassCleanup] == null ? null : methods[Methods.ClassCleanup].GetMethodInfo(); }
		}

		public override string ToString()
		{
			return Name;
		}

		#region Nested type: Methods

		internal enum Methods
		{
			/// <summary>
			/// Initialize method.
			/// </summary>
			ClassInitialize,

			/// <summary>
			/// Cleanup method.
			/// </summary>
			ClassCleanup,

			/// <summary>
			/// Test init method.
			/// </summary>
			TestInitialize,

			/// <summary>
			/// Test cleanup method.
			/// </summary>
			TestCleanup,
		}

		#endregion
	}
}