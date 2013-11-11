// -----------------------------------------------------------------------
//  <copyright file="TransactionalStorageTestBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Storage
{
	using System.Collections.Generic;

	public abstract class TransactionalStorageTestBase : RavenTest
	{
		public static IEnumerable<object[]> Storages
		{
			get
			{
				return new[]
				       {
					       new object[] { "voron" }, 
						   //new object[] { "munin" }, 
						   new object[] { "esent" }
				       };
			}
		}
	}
}