function Get-DestinationFilePath {
    <#
        .SYNOPSIS
            Accepts a source and destination file paths and a file (that from the source path) and returns the equivalent destination path (regardless of whether it exists).

        .PARAMETER File
            The file to modify.

        .PARAMETER Source
            The source path (this should form part of the base path of the -File).

        .PARAMETER Destination
            The destination path.

        .EXAMPLE
            Get-DestinationFilePath -File (Get-ChildItem c:\temp\somefile.txt) -Source c:\temp -Destination d:\example
    #>
    [cmdletbinding()]
    param(
        [Parameter(Mandatory)]
        [System.IO.FileInfo]
        $File,

        [Parameter(Mandatory)]
        [String]
        $Source,

        [Parameter(Mandatory)]
        [String]
        $Destination
    )

    $Source = Join-Path -Path $Source -ChildPath '/'
    $DestFile = Join-Path (Split-Path -Parent $File) -ChildPath '/'
    $DestFile = $DestFile -Replace "^$([Regex]::Escape((Convert-Path $Source)))", $Destination
    $DestFile = Join-Path -Path $DestFile -ChildPath (Split-Path -Leaf $File)

    Return $DestFile
}
