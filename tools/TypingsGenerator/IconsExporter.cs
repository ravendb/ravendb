﻿using System.IO;
using System.Linq;
using System.Text;

namespace TypingsGenerator
{
    public class IconsExporter
    {
        private const string TargetFile = "icons.ts";

        public void Create(string studioDir, string targetDir)
        {
            var iconsNames = ScanIcons(studioDir);

            WriteIconsFile(iconsNames, targetDir);
        }

        private string[] ScanIcons(string studioDir)
        {
            var fontsDir = Path.Combine(studioDir, "wwwroot", "Content", "css", "fonts");
            var iconsDir = Path.Combine(fontsDir, "icomoon");
            
            return Directory.GetFiles(iconsDir)
                .Where(x => x.EndsWith("svg"))
                .Select(Path.GetFileNameWithoutExtension)
                .ToArray();
        }

        private void WriteIconsFile(string[] iconNames, string targetDir)
        {
            var builder = new StringBuilder();
            builder.AppendLine("// This class is autogenerated. Do NOT modify");
            builder.AppendLine("type IconName = ");

            var allIcons = string.Join(" | \r\n", iconNames.Select(x => "    \"" + x + "\""));

            builder.AppendLine(allIcons);
            
            builder.AppendLine();
            builder.AppendLine("export = IconName;");
            
            File.WriteAllText(Path.Combine(targetDir, TargetFile), builder.ToString());            
        }
    }
}