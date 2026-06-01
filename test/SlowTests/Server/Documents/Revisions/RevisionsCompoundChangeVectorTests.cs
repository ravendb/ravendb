using System;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace SlowTests.Server.Documents.Revisions
{
    // Compound-CV coverage for the revisions key-builder surface (rules A and B; see DESIGN.md §5).
    public class RevisionsCompoundChangeVectorTests : RavenTestBase
    {
        public RevisionsCompoundChangeVectorTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task BuildRevisionKey_OnCompoundCv_ProducesDistinctSlices()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var key))
                    {
                        Assert.Equal(Encoding.UTF8.GetBytes(cv.Version.AsString()), SliceBytes(key.Raw));
                        Assert.Equal(RevisionsStorage.RevisionKeySize, key.PrefixedHash.Size);
                        Assert.NotEqual(SliceBytes(key.PrefixedHash), SliceBytes(key.Raw));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task BuildRevisionKey_HashSlice_IsHashOfVersion_NotFull()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv1 = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));
                    var cv2 = BuildCompound(context, order: ("C", DbC, 99), version: ("B", DbB, 11));

                    Assert.Equal(cv1.Version.AsString(), cv2.Version.AsString());
                    Assert.NotEqual(cv1.AsString(), cv2.AsString());

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv1, out var k1))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv2, out var k2))
                    {
                        // Same cv.Version (different order prefix) -> same hash AND same etag-sum -> identical PrefixedHash.
                        Assert.Equal(SliceBytes(k1.PrefixedHash), SliceBytes(k2.PrefixedHash));
                        Assert.Equal(SliceBytes(k1.Raw), SliceBytes(k2.Raw));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task BuildRevisionTombstoneKey_OnCompoundCv_HashAndRawCompositesDiffer()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKeys(context, cv, "users/1", out RevisionKeys keys))
                    {
                        var hashComp = SliceBytes(keys.Tombstone.HashComposite);
                        var rawComp = SliceBytes(keys.Tombstone.RawComposite);

                        var docIdBytes = Encoding.UTF8.GetBytes("users/1");
                        Assert.True(hashComp.AsSpan(0, docIdBytes.Length).SequenceEqual(docIdBytes));
                        Assert.True(rawComp.AsSpan(0, docIdBytes.Length).SequenceEqual(docIdBytes));

                        Assert.Equal(SpecialChars.RecordSeparator, hashComp[docIdBytes.Length]);
                        Assert.Equal(SpecialChars.RecordSeparator, rawComp[docIdBytes.Length]);

                        Assert.Equal(docIdBytes.Length + 1 + RevisionsStorage.RevisionKeySize, hashComp.Length);
                        Assert.Equal(docIdBytes.Length + 1 + Encoding.UTF8.GetByteCount(cv.Version.AsString()), rawComp.Length);

                        Assert.NotEqual(hashComp, rawComp);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task BuildRevisionTombstoneKey_TrailingSegment_IsVersionForm_NotFull()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKeys(context, cv, "users/1", out RevisionKeys keys))
                    {
                        var rawComp = SliceBytes(keys.Tombstone.RawComposite);
                        var rsIndex = Array.IndexOf(rawComp, SpecialChars.RecordSeparator);
                        var trailing = rawComp.AsSpan(rsIndex + 1).ToArray();

                        Assert.Equal(Encoding.UTF8.GetBytes(cv.Version.AsString()), trailing);
                        Assert.NotEqual(Encoding.UTF8.GetBytes(cv.AsString()), trailing);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task BuildRevisionAttachmentPrefix_OnCompoundCv_HashAndRawDiffer()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (Slice.From(context.Allocator, "users/1", out Slice lowerIdSlice))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var parentRevKey))
                    using (AttachmentsStorage.BuildRevisionAttachmentPrefix(context, in parentRevKey, lowerIdSlice, out var pair))
                    {
                        var hashBytes = SliceBytes(pair.HashComposite);
                        var rawBytes = SliceBytes(pair.RawComposite);
                        Assert.NotEqual(hashBytes, rawBytes);

                        var sharedPrefix = Encoding.UTF8.GetBytes("users/1");
                        Assert.True(hashBytes.AsSpan(0, sharedPrefix.Length).SequenceEqual(sharedPrefix));
                        Assert.True(rawBytes.AsSpan(0, sharedPrefix.Length).SequenceEqual(sharedPrefix));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Attachments)]
        public async Task BuildRevisionAttachmentPrefix_RevCvSlot_IsVersion_NotFull()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (Slice.From(context.Allocator, "users/1", out Slice lowerIdSlice))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var parentRevKey))
                    using (AttachmentsStorage.BuildRevisionAttachmentPrefix(context, in parentRevKey, lowerIdSlice, out var pair))
                    {
                        // Layout: [lowerDocId][RS]['r'][RS][parentRevCv.Version][RS]
                        var rawBytes = SliceBytes(pair.RawComposite);
                        Assert.Equal(SpecialChars.RecordSeparator, rawBytes[^1]);

                        int firstRs = Array.IndexOf(rawBytes, (byte)SpecialChars.RecordSeparator);
                        int revCvStart = firstRs + 3;
                        int revCvEnd = Array.IndexOf(rawBytes, (byte)SpecialChars.RecordSeparator, revCvStart);
                        Assert.True(revCvEnd > revCvStart, "Could not locate revCv segment trailing RS.");

                        var revCvSegment = rawBytes.AsSpan(revCvStart, revCvEnd - revCvStart).ToArray();

                        Assert.Equal(Encoding.UTF8.GetBytes(cv.Version.AsString()), revCvSegment);
                        Assert.NotEqual(Encoding.UTF8.GetBytes(cv.AsString()), revCvSegment);
                    }
                }
            }
        }

        // PrefixedHash is the single 24-byte PK: a 2-byte encoded etag-sum prefix ahead of the 22-byte base64 hash.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PrefixedHashSizeIs24Bytes()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var key))
                    {
                        Assert.Equal(RevisionsStorage.RevisionKeySize, key.PrefixedHash.Size);
                        Assert.Equal(RevisionsStorage.RevisionKeyHashSize,
                            key.PrefixedHash.Size - RevisionsStorage.EtagSumPrefixRawSize);
                    }
                }
            }
        }

        // The whole 24-byte PrefixedHash is base64 (it is base64([u16 BE etag-sum][Blake2b-128])),
        // so every byte -- prefix included -- is parser-safe inside a composite cv slot (never 0x1E).
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PrefixedHash_IsAll24Base64()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var key))
                    {
                        byte[] prefixed = SliceBytes(key.PrefixedHash);
                        Assert.Equal(RevisionsStorage.RevisionKeySize, prefixed.Length);
                        foreach (byte b in prefixed)
                        {
                            bool isBase64 = (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') ||
                                            (b >= 'a' && b <= 'z') || b == '+' || b == '/';
                            Assert.True(isBase64, $"PK byte 0x{b:X2} is outside the base64 alphabet.");
                        }
                    }
                }
            }
        }

    }
}
