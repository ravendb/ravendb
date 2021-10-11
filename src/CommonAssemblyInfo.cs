using System;
using System.Diagnostics;
using System.Reflection;
using System.Resources;

[assembly: AssemblyCopyright("© Hibernating Rhinos 2009 - 2021 All rights reserved.")]

[assembly: AssemblyVersion("5.3.0")]
[assembly: AssemblyFileVersion("5.3.0.53")]
[assembly: AssemblyInformationalVersion("5.3.0")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
[assembly: DebuggerDisplay("{ToString(\"O\")}", Target = typeof(DateTime))]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
