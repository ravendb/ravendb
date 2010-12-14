#!/bin/bash
MONO_IOMAP=all xbuild /property:DocumentationFile='' /property:TreatWarningsAsErrors='false' RavenDB.sln
