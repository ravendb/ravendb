// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1824.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Extensions;
using Raven.Tests.Common;
using System.Text;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1824 : NoDisposalNeeded
    {
        [Fact]
        public void VerifyMD5Core()
        {
            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHashUsingMD5Core("marcin"));

            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHashUsingMD5Core("m", "a", "r", "c", "i", "n"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHashUsingMD5Core("The MD5 message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHashUsingMD5Core("The MD5", " message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));
        }

        [Fact]
        public void VerifyMD5()
        {
            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHashUsingMD5("marcin"));

            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHashUsingMD5("m", "a", "r", "c", "i", "n"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHashUsingMD5("The MD5 message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHashUsingMD5("The MD5", " message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));
        }
        
        private string ComputeHashUsingMD5(params string[] blocks)
        {
            using (var hash = new DefaultEncryptor().CreateHash())
            {
                return ComputeHash(hash, blocks);
            }
        }

        private string ComputeHashUsingMD5Core(params string[] blocks)
        {
            using (var hash = new FipsEncryptor().CreateHash())
            {
                return ComputeHash(hash, blocks);
            }
        }

        private string ComputeHash(IHashEncryptor hash, params string[] blocks)
        {
            foreach (var block in blocks)
            {
                hash.TransformBlock(Encoding.UTF8.GetBytes(block), 0, block.Length);
            }

            return IOExtensions.GetMD5Hex(hash.TransformFinalBlock());
        }

    }
}
