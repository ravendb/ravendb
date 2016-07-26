namespace Voron
{
    public enum TransactionsMode
    {
        Safe,
        Lazy,
        Danger
    }

    public enum TransactionsModeResult
    {
        SetModeSuccessfully,
        ModeAlreadySet,
    }
}