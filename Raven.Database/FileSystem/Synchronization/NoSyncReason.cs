using System.ComponentModel;

namespace Raven.Database.Server.RavenFS.Synchronization
{
	public enum NoSyncReason
	{
		Unknown = 0,
		[Description("There were the same content and metadata")] SameContentAndMetadata = 1,
		[Description("Destination server had this file in the past")] ContainedInDestinationHistory = 2,
		[Description("File was conflicted on our side")] SourceFileConflicted = 3,
		[Description("File was conflicted on a destination side and had no resolution")] DestinationFileConflicted = 4,
		[Description("File did not exist locally")] SourceFileNotExist = 5
	}
}