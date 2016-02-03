using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

namespace Raven.Server.Web.System
{
    public class ResourceNameValidator
    {
        public static readonly string[] WindowsReservedFileNames = new string[23]
        {
            "con",
            "prn",
            "aux",
            "nul",
            "com1",
            "com2",
            "com3",
            "com4",
            "com5",
            "com6",
            "com7",
            "com8",
            "com9",
            "lpt1",
            "lpt2",
            "lpt3",
            "lpt4",
            "lpt5",
            "lpt6",
            "lpt7",
            "lpt8",
            "lpt9",
            "clock$"
        };

        public const int WindowsMaxPath = 230;

        public const int LinuxMaxFileNameLength = 230;

        public const int LinuxMaxPath = 4096;

        public static bool IsValidResourceName(string name, string dataDirectory, out string errorMessage)
        {
            if (string.IsNullOrEmpty(name))
            {
                errorMessage = "An empty name is forbidden for use!";
                return false;
            }
            if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = $"The name '{name}' contains characters that are forbidden for use!";
                return false;
            }
            if (WindowsReservedFileNames.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                errorMessage = string.Format($"The name '{name}' is forbidden for use!");
                return false;
            }
            if (Path.Combine(dataDirectory, name).Length > WindowsMaxPath)
            {
                int maxfileNameLength = WindowsMaxPath - dataDirectory.Length;
                errorMessage = $"Invalid name! Name cannot exceed {maxfileNameLength} characters";
                return false;
            }
            if ((RuntimeInformation.IsOSPlatform(OSPlatform.Linux) || RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) && 
                ((name.Length > LinuxMaxFileNameLength) ||
                (dataDirectory.Length + name.Length > LinuxMaxPath)))
            {
                int theoreticalMaxFileNameLength = LinuxMaxPath - dataDirectory.Length;
                int maxfileNameLength = theoreticalMaxFileNameLength > LinuxMaxFileNameLength ? LinuxMaxFileNameLength : theoreticalMaxFileNameLength;
                errorMessage = $"Invalid name! Name cannot exceed {maxfileNameLength} characters";
                return false;
            }

            errorMessage = null;
            return true;
        }
    }
}