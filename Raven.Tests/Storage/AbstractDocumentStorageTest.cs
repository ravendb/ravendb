//-----------------------------------------------------------------------
// <copyright file="AbstractDocumentStorageTest.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.IO;
using Raven.Database.Extensions;

namespace Raven.Tests.Storage
{
	public class AbstractDocumentStorageTest : WithDebugging, IDisposable
	{
		public AbstractDocumentStorageTest()
		{
            IOExtensions.DeleteDirectory("raven.db.test.esent");
		}

		public virtual void Dispose()
		{
            //IOExtensions.DeleteDirectory("raven.db.test.esent");
		}
	}
}
