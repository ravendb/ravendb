//-----------------------------------------------------------------------
// <copyright file="Storage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Json.Linq;
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
			using (NewTransactionalStorage())
			{
			}

			using (NewTransactionalStorage())
			{
			}
		}
	}
}