using System.IO;
using System.Linq;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Xunit;

namespace RavenFS.Tests
{
	public class StorageSignatureRepositoryTests : StorageTest
    {
        [Fact]
        public void Should_read_from_storage()
        {
            transactionalStorage.Batch(accessor =>
            {
                accessor.AddSignature("test", 1, stream => stream.Write(new byte[] { 3 }, 0, 1));
            });

            var tested = new StorageSignatureRepository(transactionalStorage, "test");
            Assert.Equal(3, tested.GetContentForReading("test.1.sig").ReadByte());
        }

        [Fact]
        public void Should_throw_FileNotFoundException_for_unknown_file()
        {
            transactionalStorage.Batch(accessor =>
            {
                accessor.AddSignature("test", 1, stream => stream.Write(new byte[] { 3 }, 0, 1));
            });

            var tested = new StorageSignatureRepository(transactionalStorage, "test");
            Assert.Throws(typeof(FileNotFoundException), () => tested.GetContentForReading("test.0.sig"));
        }

        [Fact]
        public void Should_get_SignatureInfo()
        {
            transactionalStorage.Batch(accessor =>
            {
                accessor.AddSignature("test", 1, stream => stream.Write(new byte[] { 3 }, 0, 1));
            });
            var tested = new StorageSignatureRepository(transactionalStorage, "test");
            var result = tested.GetByName("test.1.sig");
            Assert.Equal("test.1.sig", result.Name);
            Assert.Equal(1, result.Length);
        }

        [Fact]
        public void Should_assign_signature_to_proper_file()
        {
            var tested = new StorageSignatureRepository(transactionalStorage, "test.bin");
            using(var sigContent = tested.CreateContent("test.bin.0.sig"))
            {
                sigContent.WriteByte(3);
            }
            tested.Flush(new[] { SignatureInfo.Parse("test.bin.0.sig") } );

            var result = tested.GetByName("test.bin.0.sig");
            Assert.Equal("test.bin.0.sig", result.Name);
            Assert.Equal(1, result.Length);
        }

        [Fact]
        public void Should_get_SignaturInfos_by_file_name()
        {
            var tested = new StorageSignatureRepository(transactionalStorage, "test");

            transactionalStorage.Batch(accessor =>
            {
                accessor.AddSignature("test", 0, stream => stream.Write(new byte[] { 3 }, 0, 1));
                accessor.AddSignature("test", 1, stream => stream.Write(new byte[] { 3 }, 0, 1));
                accessor.AddSignature("test", 2, stream => stream.Write(new byte[] { 3 }, 0, 1));
            });

            var signatureInfos = tested.GetByFileName().ToList();
            Assert.Equal(3, signatureInfos.Count());
            foreach (var item in signatureInfos)
            {
                Assert.Equal(1, item.Length);
            }
        }
    }
}
