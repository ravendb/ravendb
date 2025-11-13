using System;
using System.Diagnostics;
using System.Reflection;
using System.Resources;

[assembly: AssemblyCopyright("© RavenDB 2009 - 2025 All rights reserved.")]

[assembly: AssemblyVersion("6.2.12")]
[assembly: AssemblyFileVersion("6.2.12.62")]
[assembly: AssemblyInformationalVersion("6.2.12")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
[assembly: DebuggerDisplay("{ToString(\"O\")}", Target = typeof(DateTime))]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
