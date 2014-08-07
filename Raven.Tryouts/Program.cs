using Raven.Client.FileSystem;
using RavenFS.Tests.ClientApi;
using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Linq.Expressions;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Listeners;
using Raven.Abstractions.FileSystem.Notifications;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Raven.Client.FileSystem.Extensions;

namespace Raven.Tryouts
{
    public class Program
	{
        protected Stream CreateUniformFileStream(int size, char value = 'a')
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string(value, size);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            return ms;
        }


        private class TakeNewestConflictsListener : IFilesConflictListener
        {
            public int ResolvedCount { get; protected set; }
            public int DetectedCount { get; protected set; }

            public ConflictResolutionStrategy ConflictDetected(FileHeader local, FileHeader remote, string destinationSourceUri)
            {
                DetectedCount++;
                if (local.LastModified.CompareTo(remote.LastModified) >= 0)
                    return ConflictResolutionStrategy.CurrentVersion;
                else
                    return ConflictResolutionStrategy.RemoteVersion;
            }

            public void ConflictResolved(FileHeader header)
            {
                ResolvedCount++;
            }

            public void Clear ()
            {
                ResolvedCount = 0;
                DetectedCount = 0;
            }
        }

        private async Task Main ()
        {
            var filesStore = new FilesStore()
            {
                Url = "http://cv-002:8080",
                DefaultFileSystem = string.Format("Test {0}", DateTime.UtcNow.Ticks),
            };
            filesStore.Initialize(true);

            var anotherStore = new FilesStore()
            {
                Url = "http://cv-002:8080",
                DefaultFileSystem = string.Format("Test-2 {0}", DateTime.UtcNow.Ticks),
            };
            anotherStore.Initialize(true);

            var conflictsListener = new TakeNewestConflictsListener();
            anotherStore.Listeners.RegisterListener(conflictsListener);

            for (int i = 0; i < 50; i++ )
            {
                conflictsListener.Clear();

                using (var sessionDestination1 = filesStore.OpenAsyncSession())
                using (var sessionDestination2 = anotherStore.OpenAsyncSession())
                {
                    sessionDestination2.RegisterUpload("test1.file", CreateUniformFileStream(130));
                    await sessionDestination2.SaveChangesAsync();

                    sessionDestination1.RegisterUpload("test1.file", CreateUniformFileStream(128));
                    await sessionDestination1.SaveChangesAsync();

                    var file = await sessionDestination1.LoadFileAsync("test1.file");
                    var file2 = await sessionDestination2.LoadFileAsync("test1.file");

                    var notificationTask = WaitForConflictNotifications(anotherStore, 2, 10);

                    var syncDestinatios = new SynchronizationDestination[] { sessionDestination2.Commands.ToSynchronizationDestination() };
                    await sessionDestination1.Commands.Synchronization.SetDestinationsAsync(syncDestinatios);
                    await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                    Debug.Assert(1 == conflictsListener.DetectedCount);

                    //We need to sync again after conflict resolution because strategy was to resolve with remote
                    await sessionDestination1.Commands.Synchronization.SynchronizeAsync();

                    await notificationTask;

                    Debug.Assert(1 == conflictsListener.ResolvedCount);

                    file = await sessionDestination1.LoadFileAsync("test1.file");
                    file2 = await sessionDestination2.LoadFileAsync("test1.file");

                    Debug.Assert(128 == file.TotalSize);
                    Debug.Assert(128 == file2.TotalSize);
                }
            }
        }

        private Task<ConflictNotification> WaitForConflictNotifications(FilesStore store, int notificationsNumber, int time)
        {
            return store.Changes()
                        .ForConflicts()
                        .OfType<ConflictNotification>()
                        .Where( x => x.Status == ConflictStatus.Resolved)
                        .Timeout(TimeSpan.FromSeconds(time))
                        .Take(notificationsNumber)
                        .ToTask();
        }


		private static void Main(string[] args)
		{
            try
            {
                var program = new Program();
                program.Main().Wait();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                Console.ReadLine();
            }
		}
	}
}