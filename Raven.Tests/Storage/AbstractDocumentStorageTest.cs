//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentStorageTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Database.Extensions;

namespace Raven.Tests.Storage
{
	public abstract class AbstractDocumentStorageTest : IDisposable
	{
		protected const string DataDir = "raven.db.test.esent";

		protected AbstractDocumentStorageTest()
		{
			IOExtensions.DeleteDirectory(DataDir);
		}

		public virtual void Dispose()
		{
			IOExtensions.DeleteDirectory(DataDir);
		}
	}
}