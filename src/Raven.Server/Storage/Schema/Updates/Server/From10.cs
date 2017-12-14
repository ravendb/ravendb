namespace Raven.Server.Storage.Schema.Updates.Server
{
    public class From10 : ISchemaUpdate
    {
        public bool Update(UpdateStep step)
        {
            step.WriteTx.DeleteFixedTree("EtagIndexName");
            return true;
        }
    }
}
