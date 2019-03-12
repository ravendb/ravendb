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

        public new bool SkipRevisionCreation
        {
#pragma warning disable 618
            get => base.SkipRevisionCreation;
            set => base.SkipRevisionCreation = value;
#pragma warning restore 618
        }
    }

    internal interface IDatabaseSmugglerImportOptions : IDatabaseSmugglerOptions
    {
    }
}
