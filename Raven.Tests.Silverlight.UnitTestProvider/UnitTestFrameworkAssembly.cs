namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Reflection;
	using Microsoft.Silverlight.Testing.Harness;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;

	public class UnitTestFrameworkAssembly : IAssembly
	{
		readonly Assembly assembly;
		readonly LazyMethodInfo cleanup;
		readonly UnitTestHarness harness;
		readonly LazyMethodInfo lazyMethodInfo;
		readonly IUnitTestProvider provider;

		public UnitTestFrameworkAssembly(IUnitTestProvider provider, UnitTestHarness unitTestHarness, Assembly assembly)
		{
			this.provider = provider;
			harness = unitTestHarness;
			this.assembly = assembly;
			lazyMethodInfo = new LazyAssemblyMethodInfo(this.assembly, ProviderAttributes.AssemblyInitialize);
			cleanup = new LazyAssemblyMethodInfo(this.assembly, ProviderAttributes.AssemblyCleanup);
		}

		public UnitTestHarness UnitTestHarness
		{
			get { return harness; }
		}

		public string Name
		{
			get
			{
				string n = assembly.ToString();
				return (n.Contains(", ") ? n.Substring(0, n.IndexOf(",", StringComparison.Ordinal)) : n);
			}
		}

		public IUnitTestProvider Provider
		{
			get { return provider; }
		}

		public MethodInfo AssemblyInitializeMethod
		{
			get { return lazyMethodInfo.GetMethodInfo(); }
		}

		public MethodInfo AssemblyCleanupMethod
		{
			get { return cleanup.GetMethodInfo(); }
		}

		public UnitTestHarness TestHarness
		{
			get { return harness; }
		}

		public ICollection<ITestClass> GetTestClasses()
		{
			var classes = from type in ReflectionUtility.GetTypesWithAttribute(assembly, ProviderAttributes.TestClass)
			              select new TestClass(this, type) as ITestClass;

			return classes.ToList();
		}
	}
}