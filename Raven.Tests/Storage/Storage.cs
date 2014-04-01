//-----------------------------------------------------------------------
// <copyright file="Storage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Storage
{
	public class Storage : RavenTest
	{
		[Fact]
		public void CanCreateNewFile()
		{
			using (NewTransactionalStorage())
			{
			}
		}

		[Fact]
		public void CanCreateNewFileAndThenOpenIt()
		{
			var dataDir = NewDataPath();

			using (NewTransactionalStorage(dataDir: dataDir))
			{
			}

			using (NewTransactionalStorage(dataDir: dataDir))
			{
			}
		}
	}
}