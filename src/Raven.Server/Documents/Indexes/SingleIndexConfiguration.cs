using System;
using Raven.Abstractions.Data;
using Raven.Client.Indexing;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes
{
    public class SingleIndexConfiguration : IndexingConfiguration
    {
        private bool? _runInMemory;
        private string _indexStoragePath;

        private readonly RavenConfiguration _databaseConfiguration;

        public SingleIndexConfiguration(IndexConfiguration clientConfiguration, RavenConfiguration databaseConfiguration)
            : base(null, null)
        {
            _databaseConfiguration = databaseConfiguration;

            Initialize(key => clientConfiguration.GetValue(key) ?? databaseConfiguration.GetSetting(key), throwIfThereIsNoSetMethod: false);

            Validate();
        }

        private void Validate()
        {
            if (string.Equals(IndexStoragePath, _databaseConfiguration.Indexing.IndexStoragePath, StringComparison.OrdinalIgnoreCase))
                return;

            if (_databaseConfiguration.Indexing.AdditionalIndexStoragePaths != null)
            {
                foreach (var path in _databaseConfiguration.Indexing.AdditionalIndexStoragePaths)
                {
                    if (string.Equals(IndexStoragePath, path, StringComparison.OrdinalIgnoreCase))
                        return;
                }
            }

            throw new InvalidOperationException($"Given index path ('{IndexStoragePath}') is not defined in '{Constants.Configuration.Indexing.StoragePath}' or '{Constants.Configuration.Indexing.AdditionalIndexStoragePaths}'");
        }

        public override bool Disabled => _databaseConfiguration.Indexing.Disabled;

        public override bool RunInMemory
        {
            get
            {
                if (_runInMemory == null)
                    _runInMemory = _databaseConfiguration.Indexing.RunInMemory;

                return _runInMemory.Value;
            }

            protected set { _runInMemory = value; }
        }

        public override string IndexStoragePath
        {
            get
            {
                if (string.IsNullOrEmpty(_indexStoragePath))
                    _indexStoragePath = _databaseConfiguration.Indexing.IndexStoragePath;

                return _indexStoragePath;
            }

            protected set
            {
                if (string.IsNullOrWhiteSpace(value))
                    return;
                _indexStoragePath = value.ToFullPath();
            }
        }
    }
}