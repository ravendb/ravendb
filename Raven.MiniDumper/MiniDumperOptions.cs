using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Raven.MiniDumper
{
    internal class MiniDumperOptions
    {
        public MiniDumperOptions()
        {
            DumpOption =
               Database.Util.MiniDumper.Option.WithThreadInfo |
               Database.Util.MiniDumper.Option.WithProcessThreadData;

            DumpPath = Path.GetTempPath();
            PdbsPath = null;
        }
        public int ProcessId { get; set; }
        public Database.Util.MiniDumper.Option DumpOption { get; set; }
        public string DumpPath { get; set; }
        public string PdbsPath { get; set; }

        public Database.Util.MiniDumper.Option SetDumpOptions(string dumpOptions)
        {
            Database.Util.MiniDumper.Option options = 0;
            var ids = dumpOptions.Split(',').ToList();
            foreach (var id in ids)
            {
                if (!string.IsNullOrEmpty(id))
                {
                    options |= Database.Util.MiniDumper.StringToOption(id);
                }
            }
            return options;
        }
    }
}
