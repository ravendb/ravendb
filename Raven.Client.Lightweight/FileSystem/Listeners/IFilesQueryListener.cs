using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem.Listeners
{
    public interface IFilesQueryListener
    {
        /// <summary>
        /// Allow to customize a query globally
        /// </summary>
        void BeforeQueryExecuted(IFilesQueryCustomization queryCustomization);
    }
}
