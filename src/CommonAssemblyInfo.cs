using System;
using System.Diagnostics;
using System.Reflection;
using System.Resources;

[assembly: AssemblyCopyright("© RavenDB 2009 - 2026 All rights reserved.")]

[assembly: AssemblyVersion("7.2.10")]
[assembly: AssemblyFileVersion("7.2.10.72")]
[assembly: AssemblyInformationalVersion("7.2.10")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
[assembly: DebuggerDisplay("{ToString(\"O\")}", Target = typeof(DateTime))]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
