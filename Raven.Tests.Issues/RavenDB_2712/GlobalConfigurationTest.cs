// -----------------------------------------------------------------------
//  <copyright file="GlobalConfigurationTest.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Database.Config.Retriever;
using Raven.Tests.Common;

namespace Raven.Tests.Issues.RavenDB_2712
{
	public class GlobalConfigurationTest : RavenTest
	{
		public GlobalConfigurationTest()
		{
			ConfigurationRetriever.EnableGlobalConfigurationOnce();
		} 
	}
}