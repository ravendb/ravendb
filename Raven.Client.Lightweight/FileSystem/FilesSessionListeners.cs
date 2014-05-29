using Raven.Client.FileSystem.Listeners;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.FileSystem
{
    public class FilesSessionListeners
    {

        /// <summary>
        ///     Create a new instance of this class
        /// </summary>
        public FilesSessionListeners()
        {
            QueryListeners = new IFilesQueryListener[0];
            WriteListeners = new IFilesWriteListener[0];
            DeleteListeners = new IFilesDeleteListener[0];
            MetadataChangeListeners = new IMetadataChangeListener[0];
            ConflictListeners = new IFilesConflictListener[0];
        }

        /// <summary>
        ///     The query listeners allow to modify queries before it is executed
        /// </summary>
        public IFilesQueryListener[] QueryListeners { get; set; }

        /// <summary>
        ///     The store listeners. You can modify the metadata before the upload happens.
        /// </summary>
        public IFilesWriteListener[] WriteListeners { get; set; }

        /// <summary>
        ///     The delete listeners.
        /// </summary>
        public IFilesDeleteListener[] DeleteListeners { get; set; }

        /// <summary>
        ///     The metadata changed listeners allow to modify metadata before it is executed.
        /// </summary>
        public IMetadataChangeListener[] MetadataChangeListeners { get; set; }

        /// <summary>
        ///     The conflict listeners
        /// </summary>
        public IFilesConflictListener[] ConflictListeners { get; set; }

        public void RegisterListener(IFilesQueryListener listener)
        {
            QueryListeners = QueryListeners.Concat(new[] { listener }).ToArray();
        }


        public void RegisterListener(IFilesWriteListener listener)
        {
            WriteListeners = WriteListeners.Concat(new[] { listener }).ToArray();
        }


        public void RegisterListener(IFilesDeleteListener listener)
        {
            DeleteListeners = DeleteListeners.Concat(new[] { listener }).ToArray();
        }


        public void RegisterListener(IMetadataChangeListener listener)
        {
            MetadataChangeListeners = MetadataChangeListeners.Concat(new[] { listener }).ToArray();
        }

        public void RegisterListener(IFilesConflictListener listener)
        {
            ConflictListeners = ConflictListeners.Concat(new[] { listener }).ToArray();
        }

    }
}
