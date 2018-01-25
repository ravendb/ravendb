
param
(
	[parameter()][string] $From,
    [parameter()][string] $To
)

function TestPath() 
{
    $FileExists = Test-Path $FolderPath
    If ($FileExists -eq $True) 
    {
        Return $true
    }
    Else 
    {
        Return $false
    }
}

function GetFilesByExtension (
	[parameter()][string] $FolderPath,
    [parameter()][string] $FileExtension
) 
{
    $Result = (TestPath($FolderPath));

    If ($Result)
    {
        $Dir = get-childitem $FolderPath -recurse
        $List = $Dir | where {$_.extension -eq $FileExtension}

        return $List
    }
    else
    {
        "Folder path is incorrect."
    }
}

$ProjectFiles = GetFilesByExtension -FolderPath "." -FileExtension ".csproj"
Foreach( $file in $ProjectFiles)
{
    $xml = [xml] (Get-Content $file.FullName)
    $modified = $false
    Foreach( $propertyGroup in $xml.Project.GetElementsByTagName("PropertyGroup") )
    {
        Foreach ( $item in $propertyGroup.GetElementsByTagName("RuntimeFrameworkVersion") )
        {
            if ($item.InnerText -Eq $From)
            {
                $item.InnerText = $To
                $modified = $true
            }
        }            
    }
    
    if ($modified)
    {
        $xml.Save($file.FullName)
    }
    
}



