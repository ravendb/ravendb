using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

[assembly: CLSCompliant(false)]

[assembly: AssemblyVersion("4.0.0")]
[assembly: AssemblyFileVersion("4.0.40.0")]
[assembly: AssemblyInformationalVersion("4.0.0 / cdc39ac / ")]

#if !DNXCORE50
[assembly: SuppressIldasm()]
#endif

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]
