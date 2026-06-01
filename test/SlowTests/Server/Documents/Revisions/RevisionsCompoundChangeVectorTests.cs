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
                        Assert.Equal(RevisionsStorage.RevisionKeyHashSize, key.Hash.Size);
                        Assert.NotEqual(SliceBytes(key.Hash), SliceBytes(key.Raw));
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
                        Assert.Equal(SliceBytes(k1.Hash), SliceBytes(k2.Hash));
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

                        Assert.Equal(docIdBytes.Length + 1 + RevisionsStorage.RevisionKeyHashSize, hashComp.Length);
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

        // Encodes the bare-hash invariant: PrefixedHash carries an extra 2-byte u16 BE prefix
        // ahead of the unchanged 22-byte bare hash. Composites embed the bare 22-byte form.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PrefixedHashSizeIs24Bytes_BareHashSizeStays22()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var key))
                    {
                        Assert.Equal(22, key.Hash.Size);
                        Assert.Equal(24, key.PrefixedHash.Size);
                    }
                }
            }
        }

        // The single-buffer property: PrefixedHash[2..24] is the same 22 bytes as Hash[0..22].
        // Asserts the "two slice views into one allocation" allocation pattern.
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task PrefixedHashSliceIsViewIntoSameBufferAsBareHash()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    var cv = BuildCompound(context, order: ("A", DbA, 7), version: ("B", DbB, 11));

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv, out var key))
                    {
                        byte[] hash = SliceBytes(key.Hash);
                        byte[] prefixed = SliceBytes(key.PrefixedHash);
                        Assert.Equal(hash, prefixed.AsSpan(2, 22).ToArray());
                    }
                }
            }
        }

        // Covers two invariants of the u16 BE etag-sum prefix:
        //   - BE encoding is load-bearing (lex order == numeric order -> B-tree locality);
        //     LE encoding would produce a different byte order for non-multiple-of-256 sums.
        //   - The prefix wraps at u16, so etag-sums differing by exactly 2^16 collide on prefix
        //     bytes but differ in the bare hash (the CV string itself differs).
        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EtagSumPrefix_IsBigEndian_AndWrapsAtU16()
        {
            using (var store = GetDocumentStore())
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                {
                    // BE: etag-sums of 1, 256, 257 -> [0x00,0x01], [0x01,0x00], [0x01,0x01].
                    // LE would give [0x01,0x00], [0x00,0x01], [0x01,0x01] -- regression detected.
                    var cv1 = BuildSingle(context, "B", DbB, etag: 1);
                    var cv256 = BuildSingle(context, "B", DbB, etag: 256);
                    var cv257 = BuildSingle(context, "B", DbB, etag: 257);
                    // Wraparound: 1 + 2^16 produces the same prefix as 1, with a different CV (and hash).
                    var cvWrap = BuildSingle(context, "B", DbB, etag: 1L + 0x10000);

                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv1, out var k1))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv256, out var k256))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cv257, out var k257))
                    using (RevisionsStorage.BuildRevisionKey(context.Allocator, cvWrap, out var kWrap))
                    {
                        byte[] p1 = SliceBytes(k1.PrefixedHash);
                        byte[] p256 = SliceBytes(k256.PrefixedHash);
                        byte[] p257 = SliceBytes(k257.PrefixedHash);
                        byte[] pWrap = SliceBytes(kWrap.PrefixedHash);

                        // BE byte order.
                        Assert.Equal(0x00, p1[0]); Assert.Equal(0x01, p1[1]);
                        Assert.Equal(0x01, p256[0]); Assert.Equal(0x00, p256[1]);
                        Assert.Equal(0x01, p257[0]); Assert.Equal(0x01, p257[1]);

                        // Wraparound: same prefix as etag=1, different bare hash.
                        Assert.Equal(p1[0], pWrap[0]);
                        Assert.Equal(p1[1], pWrap[1]);
                        Assert.NotEqual(SliceBytes(k1.Hash), SliceBytes(kWrap.Hash));
                    }
                }
            }
        }
    }
}
