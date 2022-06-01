using System.IO;
using Corax.Exceptions;
using Voron.Impl;

namespace Corax.Utils;

public class IndexVersionGuardian
{
    private const long CurrentCoraxVersion = 54_000;

    public static void AssertIndex(Transaction transaction)
    {
        var version = transaction.LowLevelTransaction.RootObjects.ReadInt64(Constants.IndexWriter.IndexVersionSlice);

        if (version.HasValue)
        {
            if (version.Value != CurrentCoraxVersion)
            {
                throw new CoraxInvalidIndexVersionException(
                    $"Index was built on Corax version {version.ToString()}. The current version {CurrentCoraxVersion} uses different structures than its predecessors. To use Corax, please restart the entire index.");
            }
            
            return;
        }
        
        throw new CoraxIndexVersionNotFound($"Corax version not found inside index. The current version {CurrentCoraxVersion} uses different structures than its predecessors. To use Corax, please restart the entire index.");
    }
    
    public static void WriteCurrentIndexVersion(Transaction transaction)
    {
        if (transaction.IsWriteTransaction == false)
        {
            throw new InvalidDataException($"Tried to write IndexVersion inside Corax. Unfortunately, transaction is readonly. ");
        }

        transaction.LowLevelTransaction.RootObjects.Add(Constants.IndexWriter.IndexVersionSlice, CurrentCoraxVersion);
    }
}
