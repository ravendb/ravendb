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
            MaxStepsForTransformScript = options.MaxStepsForTransformScript;
            OperateOnTypes = options.OperateOnTypes;
            RemoveAnalyzers = options.RemoveAnalyzers;
            TransformScript = options.TransformScript;
        }

        public bool FromCsv { get; set; }
        public string CsvCollection { get; set; }
    }

    internal interface IDatabaseSmugglerImportOptions : IDatabaseSmugglerOptions
    {
        bool FromCsv { get; set; }
        string CsvCollection { get; set; }
    }
}
