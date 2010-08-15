IronRuby - A .NET Implementation of the Ruby language

Authors: 
  Daniele Alessandri, Shri Borde, Peter Bacon Darwin, Jim Deville, 
  Curt Hagenlocher, John Lam, Haibo Luo, Tomas Matousek, John Messerly,
  Jirapong Nanta, Srivatsn Narayanan, Jimmy Schementi, Oleg Tkachenko,
  Dino Viehland, and everyone else from the community who reports bugs, 
  builds libraries, and helps enrich IronRuby.

Project Contact: Jimmy Schementi <jimmy@schementi.com>

== About

IronRuby is a Open Source implementation of the Ruby programming language
(http://www.ruby-lang.org) for .NET, heavily relying on Microsoft's 
Dynamic Language Runtime (http://dlr.codeplex.com).

The project's #1 goal is to be a true Ruby implementation, meaning it runs
existing Ruby code. See
http://ironruby.net/Documentation/Real_Ruby_Applications for information about
using the Ruby standard library and 3rd party libraries in IronRuby.

IronRuby has tightly integration with .NET, so any .NET types can be used from
IronRuby, and the IronRuby runtime can be embedded into any .NET application.
See http://ironruby.net/documentation/.net for more information.

== Running

bin/ir.exe rubyfile.rb

Will run rubyfile.rb with the IronRuby compiler.

== Package

  /bin                IronRuby binaries, ir.exe, iirb, irake, igem, iri, irdoc, etc.
  /lib                Ruby standard library, including RubyGems
  /silverlight        Silverlight binaries and scripts
  CHANGELOG.txt       Changes for each release
  RELEASE.txt         Release notes
  LICENSE.Ruby.txt    Ruby license
  LICENSE.CPL.txt     Common Public License
  LICENSE.APACHE.html Apache License, Version 2.0
  README.txt          This file

== License

Read the License.* files
