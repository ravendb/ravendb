using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class TaskProcessErrors
{
    public static TableSchema Current => TaskProcessErrorsSchemaBase;

    public static TableSchema TaskProcessErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;

    public const string TaskProcessErrorsTree = "ProcessErrors";

    public static class TaskProcessErrorsTable
    {
        public const int IdIndex = 0;
        public const int CreatedAtIndex = 1;
        public const int AffectedDocumentsCountIndex = 2;
        public const int StepIndex = 3;
        public const int ErrorIndex = 4;
    }

    static TaskProcessErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
        }

        TaskProcessErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = TaskProcessErrorsTable.IdIndex,
            Count = 1
        });

        TaskProcessErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = TaskProcessErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
    }
}
