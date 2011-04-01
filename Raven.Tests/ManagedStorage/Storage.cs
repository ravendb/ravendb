//-----------------------------------------------------------------------
// <copyright file="Storage.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.IO;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.ManagedStorage
{
	public class Storage : TxStorageTest
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

		[Fact]
		public void CanHandleTruncatedFile()
		{
			var fileName = Path.Combine("test", "raven.ravendb");
			long lengthAfterFirstTransaction;
			using (var tx = NewTransactionalStorage())
			{
				tx.Batch(mutator => mutator.Documents.AddDocument("Ayende", null, RavenJObject.FromObject(new { Name = "Rahien" }), new RavenJObject()));

				lengthAfterFirstTransaction = new FileInfo(fileName).Length;

                tx.Batch(mutator => mutator.Documents.AddDocument("Oren", null, RavenJObject.FromObject(new { Name = "Eini" }), new RavenJObject()));

			}

			using (var fileStream = File.Open(fileName, FileMode.Open))//simulate crash in the middle of a transaction write
			{
				fileStream.SetLength(lengthAfterFirstTransaction + (fileStream.Length - lengthAfterFirstTransaction) / 2);
			}

			using (var tx = NewTransactionalStorage())
			{
                tx.Batch(viewer => Assert.NotNull(viewer.Documents.DocumentByKey("Ayende", null)));
                tx.Batch(viewer => Assert.Null(viewer.Documents.DocumentByKey("Oren", null)));
			}
		}

	}
}