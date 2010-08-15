IronRuby Tutorial
=================

Description
-----------

The application is an interactive tutorial, allowing users to use a REPL window to
follow along the teaching material

Topics covered
--------------

- Creating WPF UI using XAML
  - Using Blend for UI design
  - Creating WPF FlowDocument from RDoc SimpleMarkup text
- Creating domain-specific-languages (DSLs) in Ruby
- Creating an application that can be developed incrementally from an
  interactive session with ability to reload modified source files.
- Using a splash screen at application startup

Running the app
---------------

On the desktop:

    tutorial.bat

On Silverlight:

    tutorial-sl.bat

Running the app interactively
-----------------------------

Launch ir.exe:

    load "wpf_tutorial.rb"
    #=> true
    # Edit wpf_tutorial.rb. For example, change the settings on the window in
    # the XAML
    reload # This should show the new window now...
    #=> true

Running the tests
-----------------

Both desktop and Silverlight:

    tutorial-test.bat

Just desktop:

    rake tutorial:test:desktop

Just Silverlight:

    rake tutorial:test:silverlight

