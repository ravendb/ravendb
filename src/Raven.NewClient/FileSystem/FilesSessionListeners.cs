using Raven.NewClient.Client.FileSystem.Listeners;
using System.Linq;

namespace Raven.NewClient.Client.FileSystem
{
    public class FilesSessionListeners
    {

        /// <summary>
        ///     Create a new instance of this class
        /// </summary>
        public FilesSessionListeners()
        {
            DeleteListeners = new IFilesDeleteListener[0];
            MetadataChangeListeners = new IMetadataChangeListener[0];
            ConflictListeners = new IFilesConflictListener[0];
        }

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
