using System;
using System.ComponentModel;

namespace Metrics.Utils
{
    /// <summary>
    /// Helper interface to cleanup editor visible members on metrics.
    /// </summary>
    public interface IHideObjectMembers
    {
        [EditorBrowsable(EditorBrowsableState.Never)]
        bool Equals(object obj);
        [EditorBrowsable(EditorBrowsableState.Never)]
        int GetHashCode();
        [EditorBrowsable(EditorBrowsableState.Never)]
        Type GetType();
        [EditorBrowsable(EditorBrowsableState.Never)]
        string ToString();
    }
}
