namespace Voron.Debugging
{
	public enum DebugActionType
	{
		Add,
		Delete,
		MultiAdd,
		MultiDelete,
		CreateTree,
		Increment,
		AddStruct,

        TransactionStart,
        TransactionCommit,
        TransactionRollback,
        TransactionDisposing,

        FlushStart,
        FlushEnd
	}
}
