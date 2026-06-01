using System;
using System.Collections.Generic;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Revisions;
using Tests.Infrastructure;
using Xunit;
using static Tests.Infrastructure.Utils.RevisionTestHelpers;

namespace FastTests.Server.Documents.Revisions
{
    // Locks the load-bearing properties of the revision bare-hash:
    //   * tag-blind   -- "A:e-d", "SINK:e-d", "B:e-d", "TRXN:e-d" all hash identically
    //   * order-invariant -- entry permutations of the same (DbId, Etag) set hash identically
    //   * (DbId, Etag) load-bearing -- changing either component changes the hash
    //
    // These properties make the hash stable across replication retags
    // (ReplaceKnownSinkEntries / ReplaceUnknownEntriesWithSinkTag); the receiver no longer
    // has to coordinate with the writer on which tag form was stored.
    public class RevisionHashInvariantTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_IsTagBlind_AcrossKnownTags()
        {
            byte[] hashA    = HashOf($"A:7-{DbA}");
            byte[] hashSink = HashOf($"SINK:7-{DbA}");
            byte[] hashB    = HashOf($"B:7-{DbA}");
            byte[] hashTrxn = HashOf($"TRXN:7-{DbA}");

            Assert.Equal(hashA, hashSink);
            Assert.Equal(hashSink, hashB);
            Assert.Equal(hashB, hashTrxn);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_IsOrderInvariant_AcrossEntryPermutations()
        {
            // Two entries in opposite orders -- same (DbId, Etag) set.
            byte[] ab = HashOf($"A:7-{DbA},B:11-{DbB}");
            byte[] ba = HashOf($"B:11-{DbB},A:7-{DbA}");

            Assert.Equal(ab, ba);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_IsOrderInvariant_AcrossTagsAndOrder()
        {
            // Different tags AND different order -- still the same (DbId, Etag) set.
            byte[] ordered  = HashOf($"A:7-{DbA},SINK:11-{DbB}");
            byte[] retagged = HashOf($"SINK:11-{DbB},B:7-{DbA}");

            Assert.Equal(ordered, retagged);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_DbIdIsLoadBearing()
        {
            byte[] hA = HashOf($"A:7-{DbA}");
            byte[] hB = HashOf($"A:7-{DbB}");

            Assert.NotEqual(hA, hB);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_EtagIsLoadBearing()
        {
            byte[] h7 = HashOf($"A:7-{DbA}");
            byte[] h8 = HashOf($"A:8-{DbA}");

            Assert.NotEqual(h7, h8);
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public void Hash_ShapeIs22Base64Bytes()
        {
            byte[] hash = HashOf($"A:7-{DbA}");

            Assert.Equal(RevisionsStorage.RevisionKeyHashSize, hash.Length);
            foreach (byte b in hash)
            {
                bool isBase64 = (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') ||
                                (b >= 'a' && b <= 'z') || b == '+' || b == '/';
                Assert.True(isBase64, $"Hash byte 0x{b:X2} is outside the base64 alphabet.");
            }
        }

        // Calls production directly: parse the version string, then the same
        // ComputeBareRevisionHash that GetRevisionKeyHashSlice uses.
        private static byte[] HashOf(string versionString)
        {
            List<ChangeVectorEntry> entries = versionString.ToChangeVectorList();
            Span<byte> dest = stackalloc byte[RevisionsStorage.RevisionKeyHashSize];
            RevisionsStorage.ComputeBareRevisionHash(entries, dest, out _);
            return dest.ToArray();
        }
    }
}
