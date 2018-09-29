using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using Raven.Client;
using Raven.Server.Utils;

namespace Raven.Server.Web.System
{
    public class ResourceNameValidator
    {
        public static readonly string[] WindowsReservedFileNames = {
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

        // from https://github.com/dotnet/corefx/blob/9c06da6a34fcefa6fb37776ac57b80730e37387c/src/Common/src/System/IO/PathInternal.Windows.cs#L52
        public static readonly int WindowsMaxPath = short.MaxValue;

        public const int LinuxMaxFileNameLength = 230;

        public const int LinuxMaxPath = 4096;

        public static bool IsValidResourceName(string name, string dataDirectory, out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errorMessage = "An empty name is forbidden for use!";
                return false;
            }
            if (NameUtils.IsValidResourceName(name) == false)
            {
                errorMessage = $"The name '{name}' is not permitted. Only letters, digits and characters ('_', '.', '-') are allowed.";
                return false;
            }
            if (name.Length > Constants.Documents.MaxDatabaseNameLength)
            {
                errorMessage = $"The name '{name}' exceeds '{Constants.Documents.MaxDatabaseNameLength}' characters!";
                return false;
            }
            if (name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            {
                errorMessage = $"The name '{name}' contains characters that are forbidden for use!";
                return false;
            }
            if (WindowsReservedFileNames.Any(x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase)))
            {
                errorMessage = $"The name '{name}' is forbidden for use!";
                return false;
            }

            dataDirectory = dataDirectory ?? string.Empty;
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
