namespace Voron.Impl.Journal
{
    public interface IJournalCompressionBufferCryptoHandler
    {
        void ZeroCompressionBuffer(IPagerLevelTransactionState tx);
    }
}
