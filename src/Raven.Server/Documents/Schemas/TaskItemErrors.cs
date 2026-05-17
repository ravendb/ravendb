using Sparrow.Server;
using Voron;
using Voron.Data.Tables;

namespace Raven.Server.Documents.Schemas;

public static class TaskItemErrors
{
    public static TableSchema Current => TaskItemErrorsSchemaBase;

    public static TableSchema TaskItemErrorsSchemaBase = new();

    public static readonly Slice ByCreatedAt;

    public const string TaskItemErrorsTree = "ItemErrors";

    public static class TaskItemErrorsTable
    {
        public const int DocumentIdIndex = 0;
        public const int CreatedAtIndex = 1;
        public const int StepIndex = 2;
        public const int ErrorIndex = 3;
    }

    static TaskItemErrors()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "ByCreatedAt", ByteStringType.Immutable, out ByCreatedAt);
        }

        TaskItemErrorsSchemaBase.DefineKey(new TableSchema.IndexDef
        {
            StartIndex = TaskItemErrorsTable.DocumentIdIndex,
            Count = 1
        });

        TaskItemErrorsSchemaBase.DefineIndex(new TableSchema.IndexDef // might be the same ticks, so duplicates are allowed - cannot use fixed size index
        {
            StartIndex = TaskItemErrorsTable.CreatedAtIndex,
            Name = ByCreatedAt
        });
    }
}
