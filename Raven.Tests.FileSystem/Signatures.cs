using System;
using System.IO;
using Xunit;
using System.Linq;

namespace RavenFS.Tests
{
	public class Signatures : StorageTest
	{
		[Fact]
		public void CanAddSignature()
		{
			transactionalStorage.Batch(accessor =>
			{
				accessor.AddSignature("test", 1, stream => stream.Write(new byte[]{3},0,1));
			});

			transactionalStorage.Batch(accessor =>
			{
				var signatureLevels = accessor.GetSignatures("test").ToArray();
				Assert.Equal(1, signatureLevels.Count());
				Assert.Equal(1, accessor.GetSignatureSize(signatureLevels[0].Id, signatureLevels[0].Level));
			});
		}

		[Fact]
		public void CanReadSignaturesBack()
		{
			var buffer = new byte[17 * 1024];
			new Random().NextBytes(buffer);
			transactionalStorage.Batch(accessor =>
			{
				accessor.AddSignature("test", 1, stream => stream.Write(buffer, 0, buffer.Length));
			});

			transactionalStorage.Batch(accessor =>
			{
				var signatureLevels = accessor.GetSignatures("test").ToArray();
				Assert.Equal(1, signatureLevels.Count());
				

				accessor.GetSignatureStream(signatureLevels[0].Id, signatureLevels[0].Level, stream =>
				{
					var memoryStream = new MemoryStream();
					stream.CopyTo(memoryStream);
					Assert.True(buffer.SequenceEqual(memoryStream.ToArray()));
				});
			});
		}

		[Fact]
		public void CanClearSigs()
		{
			var buffer = new byte[17 * 1024];
			new Random().NextBytes(buffer);
			transactionalStorage.Batch(accessor =>
			{
				accessor.AddSignature("test", 1, stream => stream.Write(buffer, 0, buffer.Length));
			});

			transactionalStorage.Batch(accessor =>
			{
				accessor.ClearSignatures("test");
			});

			transactionalStorage.Batch(accessor =>
			{
				var signatureLevels = accessor.GetSignatures("test").ToArray();
				Assert.Equal(0, signatureLevels.Count());
			});
		}
	}
}