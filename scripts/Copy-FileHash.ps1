function Copy-FileHash {
    <#
        .SYNOPSIS
            Copies files from one location to another based on determining change via computed hash value.

        .DESCRIPTION
            The Copy-FileHash cmdlet uses the Get-FileHash cmdlet to compute the hash value of one or more files and then copies any changed
            and new files to the specified destination path. If you use the -Recurse parameter the cmdlet will synchronise a full directory
            tree, preserving the structure and creating any missing directories in the destination path as required.

            The purpose of this cmdlet is to copy specific file changes between two paths in situations where you cannot rely on the modified
            date of the files to determine if a file has changed. This can occur in situations where file modified dates have been changed, such
            as when cloning a set of files from a source control system.

        .PARAMETER Path
            The path to the source file/s or folder/s to copy any new or changed files from.

        .PARAMETER LiteralPath
            The literal path to the source file/s or folder/s to copy any new or changed files from. Unlike the Path parameter, the value of
            LiteralPath is used exactly as it is typed. No characters are interpreted as wildcards.

        .PARAMETER Destination
            The Destination folder to compare to -Path and overwrite with any changed or new files from -Path. If the folder does not exist
            It will be created.

        .PARAMETER Algorithm
            Specifies the cryptographic hash function to use for computing the hash value of the contents of the specified file. A cryptographic
            hash function includes the property that it is not possible to find two distinct inputs that generate the same hash values. Hash
            functions are commonly used with digital signatures and for data integrity. The acceptable values for this parameter are:

            SHA1 | SHA256 | SHA384 | SHA512 | MACTripleDES | MD5 | RIPEMD160

            If no value is specified, or if the parameter is omitted, the default value is SHA256.

        .PARAMETER PassThru
            Returns the output of the file copy as an object. By default, this cmdlet does not generate any output.

        .PARAMETER Recurse
            Indicates that this cmdlet performs a recursive copy.

        .PARAMETER Force
            Indicates that this cmdlet will copy items that cannot otherwise be changed, such as copying over a read-only file or alias.

        .EXAMPLE
            Copy-FileHash -Path C:\Some\Files -Destination D:\Some\Other\Files -Recurse

            Compares the files between the two trees and replaces in the destination any where they have different contents as determined
            via hash value comparison.
    #>
    [cmdletbinding(SupportsShouldProcess)]
    param(
        [Parameter(Mandatory, ValueFromPipeline, ValueFromPipelineByPropertyName, ParameterSetName = 'Path')]
        [ValidateScript( {if (Test-Path $_) {$True} Else { Throw '-Path must be a valid path.'} })]
        [String[]]
        $Path,

        [Parameter(Mandatory, ValueFromPipelineByPropertyName, ParameterSetName = 'LiteralPath')]
        [ValidateScript( {if (Test-Path $_) {$True} Else { Throw '-LiteralPath must be a valid path.'} })]
        [String[]]
        $LiteralPath,

        [Parameter(Mandatory)]
        [ValidateScript( {if (Test-Path $_ -PathType Container -IsValid) {$True} Else { Throw '-Destination must be a valid path.' } })]
        [String]
        $Destination,

        [ValidateSet('SHA1', 'SHA256', 'SHA384', 'SHA512', 'MACTripleDES', 'MD5', 'RIPEMD160')]
        [String]
        $Algorithm = 'SHA256',

        [switch]
        $PassThru,

        [switch]
        $Recurse,

        [switch]
        $Force,

        [switch]
        $ThrowForDlls
    )
    Begin {
        Try {
            $SourcePath = If ($PSBoundParameters.ContainsKey('LiteralPath')) {
                (Resolve-Path -LiteralPath $LiteralPath).Path
            }
            Else {
                (Resolve-Path -Path $Path).Path
            }

            If (-Not (Test-Path $Destination)) {
                New-Item -Path $Destination -ItemType Container | Out-Null
                Write-Warning "$Destination did not exist and has been created as a folder path."
            }

            $Destination = Join-Path ((Resolve-Path -Path $Destination).Path) -ChildPath '/'
        }
        Catch {
            Throw $_
        }
    }
    Process {
        ForEach ($Source in $SourcePath) {
            $SourceFiles = (Get-ChildItem -Path $Source -Recurse:$Recurse -File).FullName

            ForEach ($SourceFile in $SourceFiles) {
                $SourceExtension = [System.IO.Path]::GetExtension("$SourceFile")
                $DestFile = Get-DestinationFilePath -File $SourceFile -Source $SourcePath -Destination $Destination
                $SourceHash = (Get-FileHash $SourceFile -Algorithm $Algorithm).hash

                If (Test-Path $DestFile) {
                    $DestHash = (Get-FileHash $DestFile -Algorithm $Algorithm).hash
                }
                Else {
                    #Using New-Item -Force creates an initial destination file along with any folders missing from its path.
                    #We use (Get-Date).Ticks to give the file a random value so that it is copied even if the source file is
                    #empty, so that if -PassThru has been used it is returned.
                    If ($PSCmdlet.ShouldProcess($DestFile, 'New-Item')) {
                        New-Item -Path $DestFile -Value (Get-Date).Ticks -Force -ItemType 'file' | Out-Null
                    }
                    $DestHash = $null
                }

                If (($SourceHash -ne $DestHash) -and $PSCmdlet.ShouldProcess($SourceFile, 'Copy-Item')) {
                    If ($ThrowForDlls -eq $true -and $SourceExtension -eq ".dll" -and $DestHash -ne $null) {
                        Throw "Could not copy file $SourceFile to $DestFile. Source hash: $SourceHash. Destination hash: $DestHash"
                    }
                    Else {
                        Copy-Item -Path $SourceFile -Destination $DestFile -Force:$Force -PassThru:$PassThru
                    }
                }
            }
        }
    }
}
