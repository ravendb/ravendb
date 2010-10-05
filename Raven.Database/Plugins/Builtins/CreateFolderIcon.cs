using System;
using System.IO;
using Raven.Database.Extensions;

namespace Raven.Database.Plugins.Builtins
{
    public class CreateFolderIcon : IStartupTask
    {
        public void Execute(DocumentDatabase database)
        {
            var dataDirectory = Path.GetFullPath(database.Configuration.DataDirectory);
            
            var desktopIni = Path.Combine(dataDirectory, "desktop.ini");
            var icon = Path.Combine(dataDirectory, "raven-data.ico");

            if (File.Exists(desktopIni) && File.Exists(icon))
                return;

            using (var iconFile = typeof(CreateFolderIcon).Assembly.GetManifestResourceStream("Raven.Database.Server.WebUI.raven-data.ico"))
            {
                File.WriteAllBytes(icon, iconFile.ReadData());
            }

            File.WriteAllText(desktopIni, string.Format(@"
[.ShellClassInfo]
IconResource={0},0
[ViewState]
Mode=
Vid=
FolderType=Generic
",
                                  icon));


            File.SetAttributes(desktopIni, FileAttributes.Hidden | FileAttributes.System | FileAttributes.Archive);
            File.SetAttributes(dataDirectory, FileAttributes.ReadOnly);

        }
    }
}