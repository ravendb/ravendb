using System;
using System.Diagnostics;
using System.Reflection;
using System.Resources;

[assembly: AssemblyCopyright("© Hibernating Rhinos 2009 - 2025 All rights reserved.")]

[assembly: AssemblyVersion("5.4.209")]
[assembly: AssemblyFileVersion("5.4.209.54")]
[assembly: AssemblyInformationalVersion("5.4.209")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
[assembly: DebuggerDisplay("{ToString(\"O\")}", Target = typeof(DateTime))]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
