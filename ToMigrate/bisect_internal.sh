#!/bin/sh

BASE_DIR=$(dirname $0)
CONFIGURATION="Debug"
SLN_FILE="${BASE_DIR}\RavenDB.sln"
XUNIT="${BASE_DIR}\Raven.Xunit.Runner\bin\\${CONFIGURATION}\Raven.Xunit.Runner.exe"

TEST_PROJECTS=( 
"${BASE_DIR}\Raven.Tests.Core\bin\\${CONFIGURATION}\Raven.Tests.Core.dll"
"${BASE_DIR}\Raven.Tests\bin\\${CONFIGURATION}\Raven.Tests.dll"
"${BASE_DIR}\Raven.Tests.Bundles\bin\\${CONFIGURATION}\Raven.Tests.Bundles.dll"
"${BASE_DIR}\Raven.Tests.Issues\bin\\${CONFIGURATION}\Raven.Tests.Issues.dll"
"${BASE_DIR}\Raven.Tests.MailingList\bin\\${CONFIGURATION}\Raven.Tests.MailingList.dll"
"${BASE_DIR}\Raven.SlowTests\bin\\${CONFIGURATION}\Raven.SlowTests.dll"
"${BASE_DIR}\Raven.DtcTests\bin\\${CONFIGURATION}\Raven.DtcTests.dll"
"${BASE_DIR}\Raven.Voron\Voron.Tests\bin\\${CONFIGURATION}\Voron.Tests.dll"
"${BASE_DIR}\Raven.Tests.FileSystem\bin\\${CONFIGURATION}\Raven.Tests.FileSystem.dll")

V4_NET_VERSION=`ls -l "${WINDIR}\Microsoft.NET\Framework" | egrep '^d' | awk '{print $9}' | egrep 'v4.0'`

c:/Windows/Microsoft.NET/Framework/${V4_NET_VERSION}/MSBuild.exe \
    //consoleloggerparameters:ErrorsOnly \
    //maxcpucount \
    //nologo \
    //property:Configuration=Debug \
    //verbosity:quiet \
	//property:nowarn="1591 1573" \
    RavenDB.sln

STATUS=$?

if [ $STATUS -ne 0 ]
    then
        exit 125
fi

for i in "${TEST_PROJECTS[@]}"
do
	$XUNIT $i $1
	STATUS=$?
	if [ $STATUS -ne 0 ]
		then
			exit $STATUS
		fi
done
