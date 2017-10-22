namespace Raven.Server.Storage.Schema
{
    public interface ISchemaUpdate
    {
        bool Update(UpdateStep step);
    }
}
