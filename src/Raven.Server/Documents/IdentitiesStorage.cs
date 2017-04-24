using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public class IdentitiesStorage
    {
        private readonly DocumentDatabase _documentDatabase;

        private static readonly Slice IdentitiesSlice;

        private readonly StringBuilder _keyBuilder = new StringBuilder();

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

        public string GetNextIdentityValueWithoutOverwritingOnExistingDocuments(string key, Table table, DocumentsOperationContext context, out int tries)
        {
            var identities = context.Transaction.InnerTransaction.ReadTree(IdentitiesStorage.IdentitiesSlice);
            var nextIdentityValue = identities.Increment(key, 1);
            var finalKey = AppendIdentityValueToKey(key, nextIdentityValue);
            Slice finalKeySlice;
            tries = 1;

            using (DocumentKeyWorker.GetSliceFromKey(context, finalKey, out finalKeySlice))
            {
                TableValueReader reader;
                if (table.ReadByKey(finalKeySlice, out reader) == false)
                {
                    return finalKey;
                }
            }

            /* We get here if the user inserted a document with a specified id.
            e.g. your identity is 100
            but you forced a put with 101
            so you are trying to insert next document and it would overwrite the one with 101 */

            var lastKnownBusy = nextIdentityValue;
            var maybeFree = nextIdentityValue * 2;
            var lastKnownFree = long.MaxValue;
            while (true)
            {
                tries++;
                finalKey = AppendIdentityValueToKey(key, maybeFree);
                using (DocumentKeyWorker.GetSliceFromKey(context, finalKey, out finalKeySlice))
                {
                    TableValueReader reader;
                    if (table.ReadByKey(finalKeySlice, out reader) == false)
                    {
                        if (lastKnownBusy + 1 == maybeFree)
                        {
                            nextIdentityValue = identities.Increment(key, lastKnownBusy);
                            return key + nextIdentityValue;
                        }
                        lastKnownFree = maybeFree;
                        maybeFree = Math.Max(maybeFree - (maybeFree - lastKnownBusy) / 2, lastKnownBusy + 1);
                    }
                    else
                    {
                        lastKnownBusy = maybeFree;
                        maybeFree = Math.Min(lastKnownFree, maybeFree * 2);
                    }
                }
            }
        }

        private string AppendIdentityValueToKey(string key, long val)
        {
            _keyBuilder.Length = 0;
            _keyBuilder.Append(key);
            _keyBuilder.Append(val);
            return _keyBuilder.ToString();
        }

        public string AppendNumericValueToKey(string key, long val)
        {
            _keyBuilder.Length = 0;
            _keyBuilder.Append(key);
            _keyBuilder[_keyBuilder.Length - 1] = '/';
            _keyBuilder.AppendFormat(CultureInfo.InvariantCulture, "{0:D19}", val);
            return _keyBuilder.ToString();
        }
    }
}