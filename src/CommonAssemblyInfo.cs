using System;
using System.Reflection;
using System.Resources;
using System.Runtime.CompilerServices;

[assembly: AssemblyCopyright("© Hibernating Rhinos 2004 - 2016 All rights reserved.")]

[assembly: CLSCompliant(false)]

[assembly: AssemblyVersion("4.0.0")]
[assembly: AssemblyFileVersion("4.0.0.40")]
[assembly: AssemblyInformationalVersion("4.0.0-custom-40")]

#if DEBUG
[assembly: AssemblyConfiguration("Debug")]
#else
[assembly: AssemblyConfiguration("Release")]
#endif

[assembly: AssemblyDelaySign(false)]
[assembly: NeutralResourcesLanguage("en-US")]

[assembly: InternalsVisibleTo("Tests.Infrastructure")]
[assembly: InternalsVisibleTo("FastTests")]
[assembly: InternalsVisibleTo("SlowTests")]
[assembly: InternalsVisibleTo("StressTests")]
[assembly: InternalsVisibleTo("RachisTests")]
[assembly: InternalsVisibleTo("TypingsGenerator")]
