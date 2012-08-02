// (c) Copyright Microsoft Corporation.
// This source is subject to the Microsoft Public License (Ms-PL).
// Please see http://go.microsoft.com/fwlink/?LinkID=131993 for details.
// All other rights reserved.
namespace Raven.Tests.Silverlight.UnitTestProvider
{
	using System;
	using System.Collections.Generic;
	using System.Reflection;
	using Microsoft.Silverlight.Testing.Harness;
	using Microsoft.Silverlight.Testing.UnitTesting.Metadata;
	using Microsoft.VisualStudio.TestTools.UnitTesting;

	public class RavenCustomProvider : IUnitTestProvider
	{
		const UnitTestProviderCapabilities MyCapabilities =
			UnitTestProviderCapabilities.AssemblySupportsCleanupMethod |
			UnitTestProviderCapabilities.AssemblySupportsInitializeMethod |
			UnitTestProviderCapabilities.ClassCanIgnore |
			UnitTestProviderCapabilities.MethodCanDescribe |
			UnitTestProviderCapabilities.MethodCanIgnore;
			////UnitTestProviderCapabilities.MethodCanHaveOwner | 
			////UnitTestProviderCapabilities.MethodCanHavePriority |
			////UnitTestProviderCapabilities.MethodCanHaveProperties |
			////UnitTestProviderCapabilities.MethodCanHaveTimeout |
			////UnitTestProviderCapabilities.MethodCanHaveWorkItems |
			////UnitTestProviderCapabilities.MethodCanCategorize |

		const string ProviderName = "Raven Custom Unit Test Provider";

		readonly Dictionary<Assembly, IAssembly> assemblyCache;

		public RavenCustomProvider()
		{
			assemblyCache = new Dictionary<Assembly, IAssembly>(2);
		}

		public bool HasCapability(UnitTestProviderCapabilities capability)
		{
			return ((capability & MyCapabilities) == capability);
		}

		public IAssembly GetUnitTestAssembly(UnitTestHarness testHarness, Assembly assemblyReference)
		{
			if (assemblyCache.ContainsKey(assemblyReference))
			{
				return assemblyCache[assemblyReference];
			}
			
			assemblyCache[assemblyReference] = new UnitTestFrameworkAssembly(this, testHarness, assemblyReference);
			return assemblyCache[assemblyReference];
		}

		public bool IsFailedAssert(Exception exception)
		{
			var et = exception.GetType();
			var vsttAsserts = typeof (AssertFailedException);
			return (et == vsttAsserts || et.IsSubclassOf(vsttAsserts));
		}

		public string Name
		{
			get { return ProviderName; }
		}

		public UnitTestProviderCapabilities Capabilities
		{
			get { return MyCapabilities; }
		}
	}
}