using System;

namespace Raven.Client.Documents.Smuggler
{
    public class DatabaseSmugglerOptions : IDatabaseSmugglerOptions
    {
        public const DatabaseItemType DefaultOperateOnTypes = DatabaseItemType.Indexes |
                                                              DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments |
                                                              DatabaseItemType.Conflicts |
                                                              DatabaseItemType.DatabaseRecord |
                                                              DatabaseItemType.Identities | DatabaseItemType.CompareExchange |
                                                              DatabaseItemType.Attachments | DatabaseItemType.Counters;

        private const int DefaultMaxStepsForTransformScript = 10 * 1000;

        public DatabaseSmugglerOptions()
        {
            OperateOnTypes = DefaultOperateOnTypes;
            MaxStepsForTransformScript = DefaultMaxStepsForTransformScript;
            IncludeExpired = true;
        }

        public DatabaseItemType OperateOnTypes { get; set; }

        public bool IncludeExpired { get; set; }

        public bool RemoveAnalyzers { get; set; }

        public string TransformScript { get; set; }

        public int MaxStepsForTransformScript { get; set; }

        public bool SkipRevisionCreation { get; set; }

        public bool Encrypted { get; set; }

        [Obsolete("Not used. Will be removed in next major version of the product.")]
        public bool ReadLegacyEtag { get; set; }
    }

    internal interface IDatabaseSmugglerOptions
    {
        DatabaseItemType OperateOnTypes { get; set; }
        bool IncludeExpired { get; set; }
        bool RemoveAnalyzers { get; set; }
        string TransformScript { get; set; }
        int MaxStepsForTransformScript { get; set; }

        [Obsolete("Not used. Will be removed in next major version of the product.")]
        bool ReadLegacyEtag { get; set; }
    }
}
