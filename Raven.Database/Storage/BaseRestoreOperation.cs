using System;
using System.IO;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Database.Config;
using Raven.Database.Extensions;

namespace Raven.Database.Storage
{
    internal abstract class BaseRestoreOperation
    {
        private const string IndexesSubfolder = "Indexes";
        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly Action<string> output;

        protected readonly string backupLocation;

        protected readonly DatabaseRestoreRequest _restoreRequest;
        protected readonly InMemoryRavenConfiguration Configuration;
        protected readonly string databaseLocation, indexLocation, indexDefinitionLocation, journalLocation;

        protected BaseRestoreOperation(DatabaseRestoreRequest restoreRequest, InMemoryRavenConfiguration configuration, InMemoryRavenConfiguration globalConfiguration, Action<string> output)
        {
            _restoreRequest = restoreRequest;
            backupLocation = restoreRequest.BackupLocation;
            databaseLocation = _restoreRequest.DatabaseLocation.ToFullPath();
            indexLocation = GenerateIndexLocation(_restoreRequest, configuration, globalConfiguration).ToFullPath();
            journalLocation = (_restoreRequest.JournalsLocation ?? _restoreRequest.DatabaseLocation).ToFullPath();
            Configuration = configuration;
            this.output = output;			
        }
        
        private string GenerateIndexLocation(DatabaseRestoreRequest databaseRestoreRequest, InMemoryRavenConfiguration configuration, InMemoryRavenConfiguration globalConfiguration)
        {
            //If we got the index location in the request use that.
            if (databaseRestoreRequest.IndexesLocation != null)
                return databaseRestoreRequest.IndexesLocation;

            if (globalConfiguration != null)
            {
                //If the system database uses the <database-name>\Indexes\ folder then we did not change the global index folder
                //We can safly create the index folder under the path of the database because this is where it is going to be looked for
                if (globalConfiguration.IndexStoragePath.EndsWith("\\System\\Indexes"))
                    return Path.Combine(_restoreRequest.DatabaseLocation, "Indexes");
                //system database restore with global config
                if (string.IsNullOrEmpty(configuration.DatabaseName))
                    return globalConfiguration.IndexStoragePath;
                //If we got here than the global config has a value for index storage path, will just use that folder
                return $"{globalConfiguration.IndexStoragePath}\\Databases\\{configuration.DatabaseName}";
            }

            return Path.Combine(_restoreRequest.DatabaseLocation, "Indexes");
        }

        public abstract void Execute();

        protected void ValidateRestorePreconditionsAndReturnLogsPath(string backupFilename)
        {
            if (IsValidBackup(backupFilename) == false)
            {
                output("Error: " + backupLocation + " doesn't look like a valid backup");
                output("Error: Restore Canceled");
                throw new InvalidOperationException(backupLocation + " doesn't look like a valid backup");
            }

            if (Directory.Exists(databaseLocation) && Directory.GetFileSystemEntries(databaseLocation).Length > 0)
            {
                output("Error: Database location directory is not empty. Point to non-existing or empty directory.");
                output("Error: Restore Canceled");
                throw new IOException("Database location directory is not empty. Point to non-existing or empty directory.");
            }

            CheckBackupOwner();

            if (Directory.Exists(databaseLocation) == false)
                Directory.CreateDirectory(databaseLocation);

            if (Directory.Exists(indexLocation) == false)
                Directory.CreateDirectory(indexLocation);

            if (Directory.Exists(journalLocation) == false)
                Directory.CreateDirectory(journalLocation);
        }

        protected abstract bool IsValidBackup(string backupFilename);

        protected virtual void CheckBackupOwner()
        {
        }

        protected string BackupIndexesPath()
        {
            return Path.Combine(backupLocation, "Indexes");
        }

        private void CopyAll(DirectoryInfo source, DirectoryInfo target)
        {
            // Check if the target directory exists, if not, create it.
            if (Directory.Exists(target.FullName) == false)
            {
                Directory.CreateDirectory(target.FullName);
            }

            // Copy each file into it's new directory.
            foreach (FileInfo fi in source.GetFiles())
            {
                output(string.Format(@"Copying {0}\{1}", target.FullName, fi.Name));
                Console.WriteLine(@"Copying {0}\{1}", target.FullName, fi.Name);
                fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);
            }

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
            {
                DirectoryInfo nextTargetSubDir =
                    target.CreateSubdirectory(diSourceSubDir.Name);
                CopyAll(diSourceSubDir, nextTargetSubDir);
            }
        }

        private void ForceIndexReset(string indexPath, string indexName, Exception ex)
        {
            if (Directory.Exists(indexPath))
                IOExtensions.DeleteDirectory(indexPath); // this will force index reset

            output(
                string.Format(
                    "Error: Index {0} could not be restored. All already copied index files was deleted. " +
                    "Index will be recreated after launching Raven instance. Thrown exception:{1}{2}",
                    indexName, Environment.NewLine, ex));
        }

        protected void CopyIndexDefinitions()
        {
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                                                  .OrderByDescending(dir => dir)
                                                  .ToList();

            string indexDefinitionsBackupFolder;
            string indexDefinitionsDestinationFolder = Path.Combine(databaseLocation, "IndexDefinitions");
            if (directories.Count == 0)
                indexDefinitionsBackupFolder = Path.Combine(backupLocation, "IndexDefinitions");
            else
            {
                var latestIncrementalBackupDirectory = directories.First();
                if (Directory.Exists(Path.Combine(latestIncrementalBackupDirectory, "IndexDefinitions")) == false)
                {
                    output("Failed to restore index definitions. It seems the index definitions are missing from backup folder.");
                    return;
                }
                indexDefinitionsBackupFolder = Path.Combine(latestIncrementalBackupDirectory, "IndexDefinitions");
            }

            try
            {
                CopyAll(new DirectoryInfo(indexDefinitionsBackupFolder), new DirectoryInfo(indexDefinitionsDestinationFolder));
            }
            catch (Exception ex)
            {
                output("Failed to restore index definitions. This is not supposed to happen. Reason : " + ex);
            }
        }

        protected void CopyIndexes()
        {
            var badIndexId = Directory.GetFiles(backupLocation, "*.backup_failed", SearchOption.TopDirectoryOnly).Select(Path.GetFileNameWithoutExtension).ToList();
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                                       .OrderByDescending(dir => dir)
                                       .ToList();

            if (directories.Count == 0)
            {
                if (Directory.Exists(Path.Combine(backupLocation, IndexesSubfolder)) == false)
                {
                    output(String.Format("Failed to restore index - path '{0}' doesn't exists",
                        Path.Combine(backupLocation, IndexesSubfolder)));
                    return;
                }

                foreach (var backupIndex in Directory.GetDirectories(Path.Combine(backupLocation, IndexesSubfolder)))
                {
                    var indexName = Path.GetFileName(backupIndex);                    
                    var indexPath = Path.Combine(indexLocation, indexName);
                    if (badIndexId.Contains(indexName))
                    {
                        output(string.Format("Detected a corrupt index - {0}, forcing index reset",indexName));
                        ForceIndexReset(indexPath, indexName, null);
                        continue;
                    }

                    try
                    {
                        CopyAll(new DirectoryInfo(backupIndex), new DirectoryInfo(indexPath));
                    }
                    catch (Exception ex)
                    {
                        output(string.Format("Failed to restore index, forcing index reset for {0}. Reason : {1}", indexName, ex));
                        ForceIndexReset(indexPath, indexName, ex);
                    }
                }

                return;
            }

            var latestIncrementalBackupDirectory = directories.First();
            if (Directory.Exists(Path.Combine(latestIncrementalBackupDirectory, IndexesSubfolder)) == false)
                return;

            directories.Add(backupLocation); // add the root (first full backup) to the end of the list (last place to look for)
            badIndexId = Directory.GetFiles(latestIncrementalBackupDirectory, "*.backup_failed", SearchOption.TopDirectoryOnly).Select(Path.GetFileNameWithoutExtension).ToList();
            foreach (var index in Directory.GetDirectories(Path.Combine(latestIncrementalBackupDirectory, IndexesSubfolder)))
            {
                var indexName = Path.GetFileName(index);
                var indexPath = Path.Combine(indexLocation, indexName);
                if (badIndexId.Contains(indexName))
                {
                    output(String.Format("Detected a corrupt index - {0}, forcing index reset", indexName));
                    ForceIndexReset(indexPath, indexName, null);
                    continue;
                }
                try
                {
                    var filesList = File.ReadAllLines(Path.Combine(index, "index-files.required-for-index-restore"))
                        .Where(x => string.IsNullOrEmpty(x) == false)
                        .Reverse();

                    output("Copying Index: " + indexName);

                    if (Directory.Exists(indexPath) == false)
                        Directory.CreateDirectory(indexPath);

                    foreach (var neededFile in filesList)
                    {
                        var found = false;

                        foreach (var directory in directories)
                        {
                            var possiblePathToFile = Path.Combine(directory,IndexesSubfolder , indexName, neededFile);
                            if (File.Exists(possiblePathToFile) == false)
                                continue;

                            found = true;
                            File.Copy(possiblePathToFile, Path.Combine(indexPath, neededFile));
                            break;
                        }

                        if (found == false)
                            output(string.Format("Error: File \"{0}\" is missing from index {1}", neededFile, indexName));
                    }
                }
                catch (Exception ex)
                {
                    output(string.Format("Failed to restore index, forcing index reset for {0}. Reason : {1}", indexName, ex));
                    ForceIndexReset(indexPath, indexName, ex);
                }
            }
        }

        protected string BackupFilenamePath(string backupFilename)
        {
            var directories = Directory.GetDirectories(backupLocation, "Inc*")
                .OrderByDescending(dir => dir)
                .ToList();

            var backupFilenamePath = Path.Combine(directories.Count == 0 ? backupLocation : directories.First(), backupFilename);
            return backupFilenamePath;
        }
    }
}
