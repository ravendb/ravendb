using System;
using System.IO;

namespace Raven.NewClient.Abstractions.Extensions
{
    public static class FileSystemPathExtentions
    {


        public static readonly char DirectorySeparatorChar = '\\';

        // Platform specific alternate directory separator character.  
        // This is backslash ('\') on Unix, and slash ('/') on Windows 
        // and MacOS.
        // 
        public static readonly char AltDirectorySeparatorChar = '/';

        // Platform specific volume separator character.  This is colon (':')
        // on Windows and MacOS, and slash ('/') on Unix.  This is mostly
        // useful for parsing paths like "c:\windows" or "MacVolume:System Folder".  
        // 
        public static readonly char VolumeSeparatorChar = ':';
        public static readonly char[] InvalidPathChars = { ':', '?', '*', '\"', '<', '>', '|', '\0', (Char)1, (Char)2, (Char)3, (Char)4, (Char)5, (Char)6, (Char)7, (Char)8, (Char)9, (Char)10, (Char)11, (Char)12, (Char)13, (Char)14, (Char)15, (Char)16, (Char)17, (Char)18, (Char)19, (Char)20, (Char)21, (Char)22, (Char)23, (Char)24, (Char)25, (Char)26, (Char)27, (Char)28, (Char)29, (Char)30, (Char)31 };
        public static bool dirEqualsVolume = (DirectorySeparatorChar == VolumeSeparatorChar);
        public static string DirectorySeparatorStr;

        internal static string CleanPath(string s)
        {
            int l = s.Length;
            int sub = 0;
            int start = 0;

            // Host prefix?
            char s0 = s[0];
            if (l > 2 && s0 == '\\' && s[1] == '\\')
            {
                start = 2;
            }

            // We are only left with root
            if (l == 1 && (s0 == DirectorySeparatorChar || s0 == AltDirectorySeparatorChar))
                return s;

            // Cleanup
            for (int i = start; i < l; i++)
            {
                char c = s[i];

                if (c != DirectorySeparatorChar && c != AltDirectorySeparatorChar)
                    continue;
                if (i + 1 == l)
                    sub++;
                else
                {
                    c = s[i + 1];
                    if (c == DirectorySeparatorChar || c == AltDirectorySeparatorChar)
                        sub++;
                }
            }

            if (sub == 0)
                return s;

            char[] copy = new char[l - sub];
            if (start != 0)
            {
                copy[0] = '\\';
                copy[1] = '\\';
            }
            for (int i = start, j = start; i < l && j < copy.Length; i++)
            {
                char c = s[i];

                if (c != DirectorySeparatorChar && c != AltDirectorySeparatorChar)
                {
                    copy[j++] = c;
                    continue;
                }

                // For non-trailing cases.
                if (j + 1 != copy.Length)
                {
                    copy[j++] = DirectorySeparatorChar;
                    for (; i < l - 1; i++)
                    {
                        c = s[i + 1];
                        if (c != DirectorySeparatorChar && c != AltDirectorySeparatorChar)
                            break;
                    }
                }
            }
            return new String(copy);
        }

        public static char[] PathSeparatorChars;

        static FileSystemPathExtentions()
        {
            DirectorySeparatorStr = DirectorySeparatorChar.ToString();
            PathSeparatorChars = new char[] {
                DirectorySeparatorChar,
                AltDirectorySeparatorChar,
                VolumeSeparatorChar
            };
        }
        public static string GetDirectoryName(string path)
        {
            // LAMESPEC: For empty string MS docs say both
            // return null AND throw exception.  Seems .NET throws.
            if (path == String.Empty)
                throw new ArgumentException("Invalid path");

            var pathRoot = GetPathRoot(path);
            if (path == null || pathRoot == null || pathRoot == path)
                return null;

            if (path.Trim().Length == 0)
                throw new ArgumentException("Argument string consists of whitespace characters only.");

            if (path.IndexOfAny(InvalidPathChars) > -1)
                throw new ArgumentException("Path contains invalid characters");

            int nLast = path.LastIndexOfAny(PathSeparatorChars);
            if (nLast == 0)
                nLast++;

            if (nLast > 0)
            {
                string ret = path.Substring(0, nLast);
                int l = ret.Length;

                if (l >= 2 && DirectorySeparatorChar == '\\' && ret[l - 1] == VolumeSeparatorChar)
                    return ret + DirectorySeparatorChar;
                else if (l == 1 && DirectorySeparatorChar == '\\' && path.Length >= 2 && path[nLast] == VolumeSeparatorChar)
                    return ret + VolumeSeparatorChar;
                else
                {
                    //
                    // Important: do not use CanonicalizePath here, use
                    // the custom CleanPath here, as this should not
                    // return absolute paths
                    //
                    return CleanPath(ret);
                }
            }

            return String.Empty;
        }
        internal static bool IsDirectorySeparator(char c)
        {
            return c == DirectorySeparatorChar || c == AltDirectorySeparatorChar;
        }

        public static bool IsPathRooted(string path)
        {
            if (path == null || path.Length == 0)
                return false;

            if (path.IndexOfAny(InvalidPathChars) != -1)
                throw new ArgumentException("Illegal characters in path.");

            char c = path[0];
            return (c == DirectorySeparatorChar ||
                c == AltDirectorySeparatorChar ||
                (!dirEqualsVolume && path.Length > 1 && path[1] == VolumeSeparatorChar));
        }

        public static string GetPathRoot(string path)
        {
            if (path == null)
                return null;

            if (path.Trim().Length == 0)
                throw new ArgumentException("The specified path is not of a legal form.");

            if (!IsPathRooted(path))
                return String.Empty;

            if (DirectorySeparatorChar == '/')
            {
                // UNIX
                return IsDirectorySeparator(path[0]) ? null : String.Empty;
            }
            else
            {
                // Windows
                int len = 2;

                if (path.Length == 1 && IsDirectorySeparator(path[0]))
                    return null;
                else if (path.Length < 2)
                    return String.Empty;

                if (IsDirectorySeparator(path[0]) && IsDirectorySeparator(path[1]))
                {
                    // UNC: \\server or \\server\share
                    // Get server
                    while (len < path.Length && !IsDirectorySeparator(path[len])) len++;

                    // Get share
                    if (len < path.Length)
                    {
                        len++;
                        while (len < path.Length && !IsDirectorySeparator(path[len])) len++;
                    }

                    return DirectorySeparatorStr +
                        DirectorySeparatorStr +
                        path.Substring(2, len - 2).Replace(AltDirectorySeparatorChar, DirectorySeparatorChar);
                }
                else if (IsDirectorySeparator(path[0]))
                {
                    // path starts with '\' or '/'
                    return DirectorySeparatorStr;
                }
                else if (path[1] == VolumeSeparatorChar)
                {
                    // C:\folder
                    if (path.Length >= 3 && (IsDirectorySeparator(path[2]))) len++;
                }
                else
                    return Directory.GetCurrentDirectory().Substring(0, 2);// + path.Substring (0, len);
                return path.Substring(0, len);
            }
        }
    }
}
