using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Streams
{
    public class SmuggleConfigurationsToStream : ISmuggleConfigurationsToDestination
    {
        private readonly Stream zipStream;

        private readonly StreamWriter streamWriter;

        public SmuggleConfigurationsToStream(ZipArchive archive)
        {
            var configurationsArchiveEntry = archive.CreateEntry(SmugglerConstants.FileSystem.ConfigurationsEntry);

            zipStream = configurationsArchiveEntry.Open();
            streamWriter = new StreamWriter(zipStream);
        }

        public void Dispose()
        {
            streamWriter.Dispose();
            zipStream.Dispose();
        }

        public Task WriteConfigurationAsync(string key, RavenJObject data)
        {
            streamWriter.WriteLine(RavenJObject.FromObject(new ConfigEntry { Name = key, Value = data }));

            return new CompletedTask();
        }
    }
}