using System;
using System.Reflection;
using System.Runtime.InteropServices;

#if !SILVERLIGHT
[assembly: SuppressIldasmAttribute()]
#endif

[assembly: CLSCompliant(true)]
[assembly: ComVisible(false)]
[assembly: AssemblyTitle("Raven Management Studio Plugin Core")]
[assembly: AssemblyDescriptionAttribute("A linq enabled document database for .NET")]
[assembly: AssemblyCompanyAttribute("Hibernating Rhinos")]
[assembly: AssemblyProductAttribute("RavenDB 1.0.0.0")]
[assembly: AssemblyCopyrightAttribute("Copyright © Hibernating Rhinos and Ayende Rahien 2004 - 2010")]
[assembly: AssemblyVersionAttribute("1.0.0.0")]
[assembly: AssemblyInformationalVersionAttribute("1.0.0.0 / abcdef0")]
[assembly: AssemblyFileVersionAttribute("1.0.0.13")]
[assembly: AssemblyDelaySignAttribute(false)]