using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Server.Documents.Includes;
using Sparrow.Server;
using Sparrow.Server.Utils;

namespace Raven.Server.Documents
{
    public static class ComputeHttpEtags
    {
        public static unsafe string ComputeEtagForDocuments(List<Document> documents, List<Document> includes)
        {
            // This method is efficient because we aren't materializing any values
            // except the change vector, which we need
            if (documents.Count == 1 && (includes == null || includes.Count == 0))
                return documents[0]?.ChangeVector ?? string.Empty;

            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            byte* state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ThrowFailToInitHash();

            foreach (var doc in documents)
            {
                HashDocumentByChangeVector(state, doc);
            }

            if (includes != null)
            {
                foreach (var doc in includes)
                {
                    if (doc is IncludeDocumentsCommand.ConflictDocument)
                        continue;

                    HashDocumentByChangeVector(state, doc);
                }
            }

            byte* final = stackalloc byte[(int)size];
            if (Sodium.crypto_generichash_final(state, final, size) != 0)
                ThrowFailedToFinalizeHash();

            var str = new string(' ', 49);
            fixed (char* p = str)
            {
                p[0] = 'H';
                p[1] = 'a';
                p[2] = 's';
                p[3] = 'h';
                p[4] = '-';
                var len = Base64.ConvertToBase64Array(p + 5, final, 0, 32);
                Debug.Assert(len == 44);
            }

            return str;
        }

        public static unsafe string ComputeEtagForRevisions(List<Document> revisions)
        {
            // This method is efficient because we aren't materializing any values
            // except the change vector, which we need
            if (revisions.Count == 1)
                return revisions[0]?.ChangeVector ?? string.Empty;

            var size = Sodium.crypto_generichash_bytes();
            Debug.Assert((int)size == 32);
            var cryptoGenerichashStatebytes = (int)Sodium.crypto_generichash_statebytes();
            byte* state = stackalloc byte[cryptoGenerichashStatebytes];
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, size) != 0)
                ThrowFailToInitHash();

            foreach (var doc in revisions)
            {
                HashDocumentByChangeVector(state, doc);
            }

            byte* final = stackalloc byte[(int)size];
            if (Sodium.crypto_generichash_final(state, final, size) != 0)
                ThrowFailedToFinalizeHash();

            var str = new string(' ', 49);
            fixed (char* p = str)
            {
                p[0] = 'H';
                p[1] = 'a';
                p[2] = 's';
                p[3] = 'h';
                p[4] = '-';
                var len = Base64.ConvertToBase64Array(p + 5, final, 0, 32);
                Debug.Assert(len == 44);
            }

            return str;
        }

        private static unsafe void HashDocumentByChangeVector(byte* state, Document document)
        {
            if (document == null)
            {
                if (Sodium.crypto_generichash_update(state, null, 0) != 0)
                    ThrowFailedToUpdateHash();
            }
            else
                HashChangeVector(state, document.ChangeVector);
        }

        private static unsafe void HashChangeVector(byte* state, string changeVector)
        {
            fixed (char* pCV = changeVector)
            {
                if (Sodium.crypto_generichash_update(state, (byte*)pCV, (ulong)(sizeof(char) * changeVector.Length)) != 0)
                    ThrowFailedToUpdateHash();
            }
        }

        private static void ThrowFailedToFinalizeHash()
        {
            throw new InvalidOperationException("Failed to finalize generic hash");
        }

        private static void ThrowFailToInitHash()
        {
            throw new InvalidOperationException("Failed to initiate generic hash");
        }

        private static void ThrowFailedToUpdateHash()
        {
            throw new InvalidOperationException("Failed to update generic hash");
        }
    }
}
