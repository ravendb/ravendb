using System;

namespace Raven.NewClient.Abstractions.FileSystem
{
    [Flags]
    public enum FilesSortOptions
    {
        Default = 0,
        Name = 1,
        Size = 2,
        LastModified = 8,

        Desc = 1024
    }
}
