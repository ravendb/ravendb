using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class Notifications
{
    public static TableSchema Current => NotificationsSchemaBase;

    public static TableSchema NotificationsSchemaBase = new();

    public static readonly Slice ByCreatedAt;

    public static readonly Slice ByPostponedUntil;
    
    public static readonly Slice ByType;

    public const string NotificationsTree = "Notifications";

    public static class NotificationsTable
    {
#pragma warning disable 169
        public const int IdIndex = 0;
        public const int CreatedAtIndex = 1;
        public const int PostponedUntilIndex = 2;
        public const int JsonIndex = 3;
        public const int TypeIndex = 4;
        public const int ReasonIndex = 5;
#pragma warning restore 169
    }

    static Notifications()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
            Slice.From(ctx, "ByPostponedUntil", ByteStringType.Immutable, out ByPostponedUntil);
            Slice.From(ctx, "ByType", ByteStringType.Immutable, out ByType);
        }

        NotificationsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = NotificationsTable.IdIndex,
            Count = 1
        });

        NotificationsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = NotificationsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });

        NotificationsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = NotificationsTable.PostponedUntilIndex,
            Name = ByPostponedUntil
        });
        
        NotificationsSchemaBase.DefineIndex(new TableSchema.IndexDef
        {
            StartIndex = NotificationsTable.TypeIndex,
            Name = ByType
        });
    }
}
