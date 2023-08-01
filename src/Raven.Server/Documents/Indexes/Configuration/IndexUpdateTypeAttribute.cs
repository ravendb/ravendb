using System;

namespace Raven.Server.Documents.Indexes.Configuration
{
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class IndexUpdateTypeAttribute : Attribute
    {
        public IndexUpdateType UpdateType { get; }

        public IndexUpdateTypeAttribute(IndexUpdateType updateType)
        {
            UpdateType = updateType;
        }
    }
}