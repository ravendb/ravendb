using System;
using System.Diagnostics;
using System.Reflection;
using System.Resources;

[assembly: AssemblyCopyright("© RavenDB 2009 - 2025 All rights reserved.")]

[assembly: AssemblyVersion("7.0.10")]
[assembly: AssemblyFileVersion("7.0.10.70")]
[assembly: AssemblyInformationalVersion("7.0.10")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
[assembly: DebuggerDisplay("{ToString(\"O\")}", Target = typeof(DateTime))]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
