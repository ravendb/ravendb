namespace Raven.Abstractions.Data
{
    public enum UuidType : byte
    {
        Documents = 1,
        Attachments = 2,
        DocumentTransactions = 3,
        MappedResults = 4,
        ReduceResults = 5,
        ScheduledReductions = 6,
        Queue = 7,
        Tasks = 8,
        Indexing = 9,
		EtagSynchronization = 10,
		DocumentReferences = 11
    }
}
