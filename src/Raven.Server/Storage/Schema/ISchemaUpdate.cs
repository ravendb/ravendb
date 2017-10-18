using Raven.Server.Documents;
using Voron.Impl;

namespace Raven.Server.Storage.Schema
{
    public interface ISchemaUpdate
    {
        bool Update(Transaction readTx, Transaction writeTx, ConfigurationStorage configurationStorage, DocumentsStorage documentsStorage);
    }
}
