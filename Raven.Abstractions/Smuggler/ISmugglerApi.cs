namespace Raven.Abstractions.Smuggler
{
    public interface ISmugglerApi
    {
        void ExportData(SmugglerOptions options);
        SmugglerStats ImportData(SmugglerOptions options);
    }
}