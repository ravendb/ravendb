using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas
{
    public static class Collections
    {
        public static TableSchema Current => CollectionsSchemaBase;

        internal static readonly TableSchema CollectionsSchemaBase = new TableSchema();

        internal static readonly Slice CollectionsSlice;

        public enum CollectionsTable
        {
            Name = 0
        }

        static Collections()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "Collections", ByteStringType.Immutable, out CollectionsSlice);
            }

            /*
            Collection schema is:
            full name
            collections are never deleted from the collections table
            */

            CollectionsSchemaBase.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = (int)CollectionsTable.Name,
                Count = 1,
                IsGlobal = false
            });
        }
    }
}
