using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Voron;

namespace Tests.Infrastructure.Utils
{
    // Shared helpers for revisions-storage tests: deterministic CV builders and slice-to-bytes
    // conversion. CV builders do not validate dbIds -- callers must keep them free of CV-reserved
    // chars (',', ':', '-', '|').
    internal static class RevisionTestHelpers
    {
        // 22-char base64 dbIds (the CV dbId slot is 22 chars).
        public const string DbA = "AAAAAAAAAAAAAAAAAAAAAA";
        public const string DbB = "BBBBBBBBBBBBBBBBBBBBBB";
        public const string DbC = "CCCCCCCCCCCCCCCCCCCCCC";

        // 44-char base64 attachment hashes for tests that need fixed-content rows.
        public const string Hash44A = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        public const string Hash44B = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
        public const string Hash44C = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC";
        public const string Hash44D = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";

        public enum Segment
        {
            Order,
            Version
        }

        public static ChangeVector BuildSingle(
            DocumentsOperationContext context,
            string nodeTag,
            string dbId,
            long etag)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (nodeTag == null) throw new ArgumentNullException(nameof(nodeTag));
            if (dbId == null) throw new ArgumentNullException(nameof(dbId));

            string cvString = ChangeVectorUtils.NewChangeVector(nodeTag, etag, dbId);
            return context.GetChangeVector(cvString);
        }

        public static ChangeVector BuildCompound(
            DocumentsOperationContext context,
            (string nodeTag, string dbId, long etag) order,
            (string nodeTag, string dbId, long etag) version)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            string orderString = ChangeVectorUtils.NewChangeVector(order.nodeTag, order.etag, order.dbId);
            string versionString = ChangeVectorUtils.NewChangeVector(version.nodeTag, version.etag, version.dbId);

            // Parameter order is `version, order` -- mirrors ChangeVector's ctor.
            return context.GetChangeVector(versionString, orderString);
        }

        // Single-form CVs stay single regardless of segment -- UpdateOrder/UpdateVersion short-circuit on IsSingle.
        public static ChangeVector Evolve(
            DocumentsOperationContext context,
            ChangeVector cv,
            Segment segment,
            string nodeTag,
            string dbId,
            long etag)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));
            if (cv == null) throw new ArgumentNullException(nameof(cv));
            if (nodeTag == null) throw new ArgumentNullException(nameof(nodeTag));
            if (dbId == null) throw new ArgumentNullException(nameof(dbId));

            return segment switch
            {
                Segment.Order => cv.UpdateOrder(nodeTag, dbId, etag, context),
                Segment.Version => cv.UpdateVersion(nodeTag, dbId, etag, context),
                _ => throw new ArgumentOutOfRangeException(nameof(segment), segment, null)
            };
        }

        // Drops the HashedRevisionPk SupportedFeatures token so the raw-form fallback stays active.
        // Pass via GetDocumentStore(new Options { ModifyDatabaseRecord = StripHashedRevisionPkToken }).
        public static readonly Action<DatabaseRecord> StripHashedRevisionPkToken = record =>
        {
            record.SupportedFeatures = new List<string>
            {
                Constants.DatabaseRecord.SupportedFeatures.ThrowRevisionKeyTooBigFix
            };
        };

        public static byte[] SliceBytes(Slice s) => s.AsReadOnlySpan().ToArray();
    }
}
