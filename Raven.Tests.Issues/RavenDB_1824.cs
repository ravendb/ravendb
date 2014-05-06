// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1824.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Security.Cryptography;
using System.Text;
using Raven.Abstractions.Util;
using Raven.Abstractions.Util.Encryptors;
using Raven.Database.Server.RavenFS.Extensions;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1824
    {
        [Fact]
        public void VerifyMD5Core()
        {
            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHash("marcin"));

            Assert.Equal("d5fad0cda8f1079681ec510bb20a586c", ComputeHash("m", "a", "r", "c", "i", "n"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHash("The MD5 message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));

            Assert.Equal("0e2ce420a49e9f242b6d113a4deea594", ComputeHash("The MD5", " message-digest algorithm is a widely used cryptographic hash function producing a 128-bit (16-byte) hash value"));
            
        } 

        public string ComputeHash(params string[] blocks)
        {
            using (var hash = new FipsEncryptor().CreateHash())
            {
                foreach (var block in blocks)
                {
                    hash.TransformBlock(Encoding.UTF8.GetBytes(block), 0, block.Length);
                }
                
                return IOExtensions.GetMD5Hex(hash.TransformFinalBlock());
            }
        }

    }
}