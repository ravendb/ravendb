// -----------------------------------------------------------------------
//  <copyright file="SignatureTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using System.Text;

using Xunit;
using Xunit.Extensions;

namespace RavenFS.Tests.Storage
{
    public class SignatureTests : StorageAccessorTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public void AddSignature(string requestedStorage)
        {
            var text1 = "text1";
            var text2 = "text2";

            var buffer1 = Encoding.UTF8.GetBytes(text1);
            var buffer2 = Encoding.UTF8.GetBytes(text2);

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.GetSignatures("signature1")));

                storage.Batch(accessor => accessor.AddSignature("signature1", 10, stream => stream.Write(buffer1, 0, buffer1.Length)));
                storage.Batch(accessor => accessor.AddSignature("signature2", 999, stream => stream.Write(buffer2, 0, buffer2.Length)));

                storage.Batch(accessor =>
                {
                    var signatures = accessor
                        .GetSignatures("signature1")
                        .ToList();

                    Assert.Equal(1, signatures.Count);
                    Assert.Equal(1, signatures[0].Id);
                    Assert.Equal(10, signatures[0].Level);
                    Assert.True((signatures[0].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);

                    signatures = accessor
                        .GetSignatures("signature2")
                        .ToList();

                    Assert.Equal(1, signatures.Count);
                    Assert.Equal(2, signatures[0].Id);
                    Assert.Equal(999, signatures[0].Level);
                    Assert.True((signatures[0].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void GetSignatures(string requestedStorage)
        {
            var text1 = "text1";
            var text2 = "text2";

            var buffer1 = Encoding.UTF8.GetBytes(text1);
            var buffer2 = Encoding.UTF8.GetBytes(text2);

            using (var storage = NewTransactionalStorage(requestedStorage))
            {
                storage.Batch(accessor => Assert.Empty(accessor.GetSignatures("signature1")));

                storage.Batch(accessor => accessor.AddSignature("signature1", 10, stream => stream.Write(buffer1, 0, buffer1.Length)));
                storage.Batch(accessor => accessor.AddSignature("signature1", 11, stream => stream.Write(buffer1, 0, buffer1.Length)));
                storage.Batch(accessor => accessor.AddSignature("signature1", 10, stream => stream.Write(buffer1, 0, buffer1.Length)));
                storage.Batch(accessor => accessor.AddSignature("signature2", 999, stream => stream.Write(buffer2, 0, buffer2.Length)));

                storage.Batch(accessor =>
                {
                    var signatures = accessor
                        .GetSignatures("signature1")
                        .ToList();

                    Assert.Equal(3, signatures.Count);

                    Assert.Equal(1, signatures[0].Id);
                    Assert.Equal(10, signatures[0].Level);
                    Assert.True((signatures[0].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);

                    Assert.Equal(2, signatures[1].Id);
                    Assert.Equal(11, signatures[1].Level);
                    Assert.True((signatures[1].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);

                    Assert.Equal(3, signatures[2].Id);
                    Assert.Equal(10, signatures[2].Level);
                    Assert.True((signatures[2].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);

                    signatures = accessor
                        .GetSignatures("signature2")
                        .ToList();

                    Assert.Equal(1, signatures.Count);
                    Assert.Equal(4, signatures[0].Id);
                    Assert.Equal(999, signatures[0].Level);
                    Assert.True((signatures[0].CreatedAt - DateTime.UtcNow).TotalMilliseconds < 10);
                });
            }
        }
    }
}