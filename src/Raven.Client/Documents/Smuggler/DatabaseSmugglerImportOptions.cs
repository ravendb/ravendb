namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerImportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerImportOptions
    {
        public DatabaseSmugglerImportOptions()
        {
        }

        public DatabaseSmugglerImportOptions(DatabaseSmugglerOptions options)
        {
            IncludeExpired = options.IncludeExpired;
            IncludeArtificial = options.IncludeArtificial;
            OptionsForTransformScript = options.OptionsForTransformScript;
            OperateOnTypes = options.OperateOnTypes;
            RemoveAnalyzers = options.RemoveAnalyzers;
            TransformScript = options.TransformScript;
        }

        public bool SkipRevisionCreation { get; set; }
    }

    internal interface IDatabaseSmugglerImportOptions : IDatabaseSmugglerOptions
    {
        bool SkipRevisionCreation { get; set; }
    }
}
