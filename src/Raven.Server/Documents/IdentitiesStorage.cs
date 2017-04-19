using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class IdentitiesStorage
    {
        private readonly DocumentDatabase _documentDatabase;
        public static readonly Slice IdentitiesSlice;

        static IdentitiesStorage()
        {
            Slice.From(StorageEnvironment.LabelsContext, "Identities", ByteStringType.Immutable, out IdentitiesSlice);
        }

        public IdentitiesStorage(DocumentDatabase documentDatabase, Transaction tx)
        {
            _documentDatabase = documentDatabase;
            tx.CreateTree(IdentitiesSlice);
        }

        public void Update(DocumentsOperationContext context, Dictionary<string, long> identities)
        {
            var readTree = context.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
            foreach (var identity in identities)
            {
                readTree.AddMax(identity.Key, identity.Value);
            }
        }

        public long IdentityFor(DocumentsOperationContext ctx, string key)
        {
            var identities = ctx.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
            return identities.Increment(key, 1);
        }

        public IEnumerable<KeyValuePair<string, long>> GetIdentities(DocumentsOperationContext context)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(IdentitiesSlice);
            using (var it = identities.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys) == false)
                    yield break;

                do
                {
                    var name = it.CurrentKey.ToString();
                    var value = it.CreateReaderForCurrent().ReadLittleEndianInt64();

                    yield return new KeyValuePair<string, long>(name, value);
                } while (it.MoveNext());
            }
        }
    }
}