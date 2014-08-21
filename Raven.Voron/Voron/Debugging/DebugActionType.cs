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

        TransactionStart,
        TransactionCommit,
        TransactionRollback,
        TransactionDisposed,

        FlushStart,
        FlushEnd
	}
}
