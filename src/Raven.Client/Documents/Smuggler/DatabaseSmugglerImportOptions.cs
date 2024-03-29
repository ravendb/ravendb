﻿namespace Raven.Client.Documents.Smuggler
{
    public sealed class DatabaseSmugglerImportOptions : DatabaseSmugglerOptions, IDatabaseSmugglerImportOptions
    {
        public DatabaseSmugglerImportOptions()
        {
        }

        public DatabaseSmugglerImportOptions(DatabaseSmugglerOptions options)
        {
            IncludeExpired = options.IncludeExpired;
            IncludeArtificial = options.IncludeArtificial;
            IncludeArchived = options.IncludeArchived;
            MaxStepsForTransformScript = options.MaxStepsForTransformScript;
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
