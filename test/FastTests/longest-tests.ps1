function Take-Count() {
    param ( [int]$count = $(throw "Need a count") )
    begin { 
        $total = 0;
    }
    process { 
        if ( $total -lt $count ) {
            $_
        }
        $total += 1
    }
}



dnx test  -xml out.xml

[xml]$tests = Get-Content out.xml
$tests.assemblies.assembly.collection.test | 
	sort @{e={$_.time -as [double]} } -descending | 
	% {$_.time + "  " + $_.name } | 
	take-count 10

