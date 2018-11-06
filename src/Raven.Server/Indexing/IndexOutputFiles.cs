using System;
using System.Collections.Generic;

namespace Raven.Server.Indexing
{
    public class IndexOutputFiles
    {
        private readonly Dictionary<string, VoronIndexOutput> _voronIndexFiles;

        public int Count => _voronIndexFiles.Count;

        public IndexOutputFiles()
        {
            _voronIndexFiles = new Dictionary<string, VoronIndexOutput>(StringComparer.OrdinalIgnoreCase);
        }

        public void Remove(string name)
        {
            _voronIndexFiles.Remove(name);
        }

        public void Add(string name, VoronIndexOutput voronIndexOutput)
        {
            _voronIndexFiles.Add(name, voronIndexOutput);
        }

        public long CalculateTotalWritten()
        {
            var totalWritten = 0L;
            foreach (var indexOutput in _voronIndexFiles)
            {
                totalWritten += indexOutput.Value.TotalWritten;
            }

            return totalWritten;
        }
    }
}
