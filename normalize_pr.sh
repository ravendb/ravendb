#!/bin/sh

against="$1"
files=`git --no-pager diff --name-only HEAD $against |
                       egrep '\.cs$|\.csproj$|\.vbproj$|\.fsproj$|\.dbproj$|\.ascx$|\.xaml$|\.cmd$|\.ps1$|\.coffee$|\.config$|\.css$|\.nuspec$|\.scss$|\.cshtml$|\.htm$|\.html$|\.js$|\.ts$|\.msbuild$|\.resx$|\.ruleset$|\.Stylecop$|\.targets$|\.tt$|\.txt$|\.vb$|\.vbhtml$|\.xml$|\.xunit$|\.java$|\.less$' |
                       uniq`

# Find files with trailing whitespace
for FILE in $files ; do
if [[ -e "$FILE" ]]; then
    echo "Fixing whitespace in $FILE"
    TMP="$FILE.tmp"
    expand --tabs=4 --initial "$FILE" > TMP
    mv TMP "$FILE"
    git add "$FILE"
fi
done